package runners

import (
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshot"
)

const QueueFullMsg = "No resources available. Please try later."
const QueueInitingMsg = "Queue is still initializing. Please try later."
const QueueInvalidMsg = "Failed initialization, queue permanently broken."

type cmdAndID struct {
	cmd *runner.Command
	id  runner.RunID
}

// NewQueueRunner creates a new Service that uses a Queue
// The init chan is optional and may be nil if the filer has no initializion step.
func NewQueueRunner(
	exec execer.Execer, filer snapshot.Filer, idc snapshot.InitDoneCh, output runner.OutputCreator, tmp *temp.TempDir, capacity int, stat stats.StatsReceiver) runner.Service {

	if stat == nil {
		stat = stats.NilStatsReceiver()
	}

	history := 1
	if capacity > 0 {
		history = 0 // unlimited if acting as a queue (vs single runner).
	}

	updateTicker := makeSimpleTicker(filer.UpdateInterval())
	statusManager := NewStatusManager(history)
	inv := NewInvoker(exec, filer, output, tmp, stat)

	controller := &QueueController{
		statusManager: statusManager,
		inv:           inv,
		filer:         filer,
		capacity:      capacity,
		startCh:       make(chan cmdAndID),
		updateCh:      updateTicker,
	}
	run := &Service{controller, statusManager, statusManager}

	// QueueRunner will not serve requests if an idc is defined and returns an error
	log.Info("Starting goroutine to check for snapshot init: ", (idc != nil))
	var err error = nil
	if idc != nil {
		go func() {
			err = <-idc
			if err != nil {
				stat.Counter(stats.WorkerDownloadInitFailure).Inc(1)
				statusManager.UpdateService(runner.ServiceStatus{Initialized: false, Error: err})
			} else {
				statusManager.UpdateService(runner.ServiceStatus{Initialized: true})
				go controller.loop()
			}
		}()
	} else {
		statusManager.UpdateService(runner.ServiceStatus{Initialized: true})
		go controller.loop()
	}

	return run
}

func NewSingleRunner(
	exec execer.Execer, filer snapshot.Filer, idc snapshot.InitDoneCh, output runner.OutputCreator, tmp *temp.TempDir, stat stats.StatsReceiver) runner.Service {
	return NewQueueRunner(exec, filer, idc, output, tmp, 0, stat)
}

// QueueController maintains a queue of commands to run (up to capacity).
// Manages updates to underlying Filer via Filer's Update interface,
// if a non-zero update interval is defined (updates and tasks cannot run concurrently)
type QueueController struct {
	inv           *Invoker
	filer         snapshot.Filer
	statusManager *StatusManager
	capacity      int

	mu           sync.Mutex
	queue        []cmdAndID
	runningID    runner.RunID
	runningAbort chan<- struct{}

	// used to signal a cmd run request
	startCh chan cmdAndID
	// used to signal a request to update the Filer
	updateCh <-chan time.Time
}

func makeSimpleTicker(d time.Duration) <-chan time.Time {
	if d != snapshot.NoDuration {
		return time.NewTicker(d).C
	}
	return nil
}

// Run enqueues the command or rejects it, returning its status or an error.
func (c *QueueController) Run(cmd *runner.Command) (runner.RunStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Note, 'capacity' is the max number of queued cmds and we allow one running job before
	//        we start queueing, so total allowed runs is actually defined as capacity+1.
	numCmds := 0
	isRunning := (c.runningID != runner.RunID(""))
	if isRunning {
		numCmds = 1 + len(c.queue)
	}

	_, svcStatus, _ := c.statusManager.StatusAll()
	log.Infof("Trying to run, ready=%t, err=%v, available slots:%d/%d, currentRun:%s, jobID:%s, taskID:%s",
		svcStatus.Initialized, svcStatus.Error, c.capacity+1-numCmds, c.capacity+1, c.runningID, cmd.JobID, cmd.TaskID)

	if !svcStatus.Initialized {
		return runner.RunStatus{Error: svcStatus.Error.Error()}, fmt.Errorf(QueueInitingMsg)
	}
	if numCmds > c.capacity {
		return runner.RunStatus{}, fmt.Errorf(QueueFullMsg)
	}
	st, err := c.statusManager.NewRun()

	if err != nil {
		return st, err
	}
	if !isRunning {
		c.runningID = st.RunID
		c.start(cmdAndID{cmd, st.RunID})
	} else {
		c.queue = append(c.queue, cmdAndID{cmd, st.RunID})
	}
	return st, nil
}

// Abort kills the given run, returning its final status.
func (c *QueueController) Abort(run runner.RunID) (runner.RunStatus, error) {
	c.mu.Lock()

	if run == c.runningID {
		if c.runningAbort != nil {
			close(c.runningAbort)
			c.runningAbort = nil
		}
	} else {
		for i, cmdID := range c.queue {
			if run == cmdID.id {
				c.queue = append(c.queue[:i], c.queue[i+1:]...)
				c.statusManager.Update(runner.AbortStatus(
					run,
					runner.LogTags{JobID: cmdID.cmd.JobID, TaskID: cmdID.cmd.TaskID}))
			}
		}
	}

	// Unlock so watch() abort can call c.statusManager.Update()
	c.mu.Unlock()
	status, _, err := runner.FinalStatus(c.statusManager, run)
	return status, err
}

// Handle requests to run and update, to provide concurrency management between the two.
// Although we can still receive run requests, runs and updates are done blocking.
func (c *QueueController) loop() {
	justUpdated := false

	for c.startCh != nil {
		// Prefer to run an update first if we have one scheduled
		select {
		case <-c.updateCh:
			if err := c.filer.Update(); err != nil {
				log.Errorf("Error running Filer Update: %v\n", err)
			}
			justUpdated = true
		default:
		}

		// Wait on update or run start.
		// If we just did an update above, just drain the updateCh and move on
		// We still have to wait on updateCh here, or we'll never update without runs requests
		select {
		case <-c.updateCh:
			if justUpdated {
				justUpdated = false
			} else {
				if err := c.filer.Update(); err != nil {
					log.Errorf("Error running Filer Update: %v\n", err)
				}
			}

		case cmdID, ok := <-c.startCh:
			if !ok {
				c.startCh = nil
				continue
			}
			c.runAndWatch(cmdID)
		}
	}
}

// Add a run cmdAndID to startCh - goroutine since loop blocks on startCh receptions
func (c *QueueController) start(cmdID cmdAndID) {
	go func() {
		c.startCh <- cmdID
	}()
}

// Blocking run of cmd
func (c *QueueController) runAndWatch(cmdID cmdAndID) {
	abortCh, statusUpdateCh := c.inv.Run(cmdID.cmd, cmdID.id)
	c.runningAbort = abortCh
	c.watch(statusUpdateCh)
}

// Watch a cmd to completion, but also the mechanism for queued cmds to be started
func (c *QueueController) watch(statusUpdateCh <-chan runner.RunStatus) {
	for st := range statusUpdateCh {
		log.Debugf("Queue pulled result:%+v\n", st)
		if st.State.IsDone() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.runningID = ""
			c.runningAbort = nil
			if len(c.queue) > 0 {
				cmdID := c.queue[0]
				c.queue = c.queue[1:]
				log.Infof("Running from queue:%+v\n", cmdID)
				c.runningID = cmdID.id
				c.start(cmdID)
			}
		}
		c.statusManager.Update(st)
	}
}
