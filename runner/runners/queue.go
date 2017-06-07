package runners

import (
	"fmt"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/common/log/hooks"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshot"
)

// Used to get proper logging from tests...
func init() {
	if loglevel := os.Getenv("SCOOT_LOGLEVEL"); loglevel != "" {
		level, err := log.ParseLevel(loglevel)
		if err != nil {
			log.Error(err)
			return
		}
		log.SetLevel(level)
		log.AddHook(hooks.NewContextHook())
	}
}

const QueueFullMsg = "No resources available. Please try later."
const QueueInitingMsg = "Queue is still initializing. Please try later."
const QueueInvalidMsg = "Failed initialization, queue permanently broken."

type result struct {
	st  runner.RunStatus
	err error
}

type runReq struct {
	cmd      *runner.Command
	resultCh chan result
}

type abortReq struct {
	runID    runner.RunID
	resultCh chan result
}

type cmdAndID struct {
	cmd *runner.Command
	id  runner.RunID
}

/*
NewQueueRunner creates a new Service that uses a Queue
If the worker has an initialization step (indicated by non-nil in idc) the queue will wait for the
worker to finish initialization before accepting any run requests.
If the queue is full when a command is received, an empty RunStatus and a queue full error will be returned.

@param: exec - runs the command
@param: filer - uses the UpdateInterval from filer to ????
@param: idc - channel that ?filer? will use to report when its initialization is done.
The init chan is optional and may be nil if the filer has no initializion step.
@param: idtCh - channel that queue uses to report (to handler) the time the initialization finished
@param: output
@param: tmp
@param: capacity - the maximum number of commands to support on the queue.  If 0 then the queue is unbounded.
@param: stats - the stats receiver the queue will use when reporting its metrics
*/
func NewQueueRunner(
	exec execer.Execer, filer snapshot.Filer, idc snapshot.InitDoneCh, idtCh snapshot.InitDoneTimeCh, output runner.OutputCreator, tmp *temp.TempDir, capacity int, stat stats.StatsReceiver) runner.Service {

	if stat == nil {
		stat = stats.NilStatsReceiver()
	}

	//FIXME(jschiller): proper history config rather than keying off of capacity and if this is a SingleRunner.
	history := 1
	if capacity > 0 {
		history = 0 // unlimited if acting as a queue (vs single runner).
	} else if capacity == 0 {
		capacity = 1 // singleRunner, override capacity so it can actually run a command.
	}

	statusManager := NewStatusManager(history)
	inv := NewInvoker(exec, filer, output, tmp, stat)

	controller := &QueueController{
		statusManager: statusManager,
		inv:           inv,
		filer:         filer,
		capacity:      capacity,
		reqCh:         make(chan interface{}),
		updateCh:      make(chan struct{}),
	}
	run := &Service{controller, statusManager, statusManager}

	// QueueRunner will not serve requests if an idc is defined and returns an error
	log.Info("Starting goroutine to check for snapshot init? ", (idc != nil))
	var err error = nil
	if idc != nil {
		go func() {
			err = <-idc
			if err != nil {
				stat.Counter(stats.WorkerDownloadInitFailure).Inc(1)
				statusManager.UpdateService(runner.ServiceStatus{Initialized: false, Error: err})
				if idtCh != nil {
					idtCh <- time.Time{}
				}
			} else {
				statusManager.UpdateService(runner.ServiceStatus{Initialized: true})
				startUpdateTicker(filer.UpdateInterval(), controller.updateCh)
				if idtCh != nil {
					idtCh <- time.Now()
				}
			}
		}()
	} else {
		statusManager.UpdateService(runner.ServiceStatus{Initialized: true})
		startUpdateTicker(filer.UpdateInterval(), controller.updateCh)
	}

	go controller.loop()

	return run
}

func NewSingleRunner(
	exec execer.Execer, filer snapshot.Filer, idc snapshot.InitDoneCh, idtCh snapshot.InitDoneTimeCh, output runner.OutputCreator, tmp *temp.TempDir, stat stats.StatsReceiver) runner.Service {
	return NewQueueRunner(exec, filer, idc, idtCh, output, tmp, 0, stat)
}

// QueueController maintains a queue of commands to run (up to capacity).
// Manages updates to underlying Filer via Filer's Update interface,
// if a non-zero update interval is defined (updates and tasks cannot run concurrently)
type QueueController struct {
	inv           *Invoker
	filer         snapshot.Filer
	statusManager *StatusManager
	capacity      int

	queue        []cmdAndID
	runningID    runner.RunID
	runningCmd   *runner.Command
	runningAbort chan<- struct{}

	// used to signal a cmd run request
	reqCh chan interface{}
	// used to signal a request to update the Filer
	updateCh chan struct{}
}

// Start a goroutine that forwards update ticks to updateCh, so that we can skip if e.g. init failed
// This doesn't support a stop mechanism, because QueueController doesn't support a stop/close semantic
func startUpdateTicker(d time.Duration, u chan<- struct{}) {
	if d == snapshot.NoDuration || u == nil {
		return
	}
	ticker := time.NewTicker(d).C
	go func(t <-chan time.Time) {
		for _ = range t {
			u <- struct{}{}
		}
	}(ticker)
}

// Run enqueues the command or rejects it, returning its status or an error.
func (c *QueueController) Run(cmd *runner.Command) (runner.RunStatus, error) {
	resultCh := make(chan result)
	c.reqCh <- runReq{cmd, resultCh}
	result := <-resultCh
	return result.st, result.err
}

func (c *QueueController) enqueue(cmd *runner.Command) (runner.RunStatus, error) {
	_, svcStatus, _ := c.statusManager.StatusAll()
	log.Infof("Trying to run, ready=%t, err=%v, available slots:%d/%d, currentRun:%s, jobID:%s, taskID:%s",
		svcStatus.Initialized, svcStatus.Error, c.capacity-len(c.queue), c.capacity, c.runningID, cmd.JobID, cmd.TaskID)

	if !svcStatus.Initialized {
		return runner.RunStatus{Error: svcStatus.Error.Error()}, fmt.Errorf(QueueInitingMsg)
	}
	if len(c.queue) >= c.capacity {
		return runner.RunStatus{}, fmt.Errorf(QueueFullMsg)
	}

	st, err := c.statusManager.NewRun()
	if err != nil {
		return st, err
	}
	c.queue = append(c.queue, cmdAndID{cmd, st.RunID})

	return st, nil
}

// Abort kills the given run, returning its final status.
func (c *QueueController) Abort(run runner.RunID) (runner.RunStatus, error) {
	resultCh := make(chan result)
	c.reqCh <- abortReq{run, resultCh}
	result := <-resultCh
	return result.st, result.err
}

func (c *QueueController) abort(run runner.RunID) (runner.RunStatus, error) {
	if run == c.runningID {
		if c.runningAbort != nil {
			log.Infof("Aborting currentRun:%s, jobID:%s, taskID:%s", c.runningID, c.runningCmd.JobID, c.runningCmd.TaskID)
			close(c.runningAbort)
			c.runningAbort = nil
		}
	} else {
		for i, cmdID := range c.queue {
			if run == cmdID.id {
				log.Infof("Aborting queued run:%s, jobID:%s, taskID:%s", run, c.runningCmd.JobID, c.runningCmd.TaskID)
				c.queue = append(c.queue[:i], c.queue[i+1:]...)
				c.statusManager.Update(runner.AbortStatus(
					run,
					runner.LogTags{JobID: cmdID.cmd.JobID, TaskID: cmdID.cmd.TaskID}))
			}
		}
	}

	status, _, err := runner.FinalStatus(c.statusManager, run)
	return status, err
}

// Handle requests to run and update, to provide concurrency management between the two.
// Although we can still receive run requests, runs and updates are done blocking.
func (c *QueueController) loop() {
	var watchCh chan runner.RunStatus
	var updateDoneCh chan interface{}
	updateRequested := false

	tryUpdate := func() {
		if updateDoneCh == nil && watchCh == nil {
			updateRequested = false
			updateDoneCh = make(chan interface{})
			go func() {
				if err := c.filer.Update(); err != nil {
					log.Errorf("Error running Filer Update: %v\n", err)
				}
				updateDoneCh <- nil
			}()
		} else {
			updateRequested = true
		}
	}

	tryRun := func() {
		if watchCh == nil && updateDoneCh == nil && len(c.queue) > 0 {
			cmdID := c.queue[0]
			watchCh = c.runAndWatch(cmdID)
		}
	}

	for c.reqCh != nil {
		// Prefer to run an update first if we have one scheduled. If not updating, try running.
		if updateRequested {
			tryUpdate()
		} else {
			select {
			case <-c.updateCh:
				tryUpdate()
			default:
				tryRun()
			}
		}

		// Wait on update, updateDone, run start, or run abort.
		select {
		case <-c.updateCh:
			tryUpdate()

		case <-updateDoneCh:
			// Give run() a chance right after we've finished updating.
			updateDoneCh = nil
			tryRun()

		case req, ok := <-c.reqCh:
			// Handle run and abort requests.
			if !ok {
				c.reqCh = nil
				continue
			}
			switch r := req.(type) {
			case runReq:
				st, err := c.enqueue(r.cmd)
				r.resultCh <- result{st, err}
			case abortReq:
				st, err := c.abort(r.runID)
				r.resultCh <- result{st, err}
			}

		case <-watchCh:
			// Handle finished run by resetting state.
			watchCh = nil
			c.runningID = ""
			c.runningCmd = nil
			c.runningAbort = nil
			c.queue = c.queue[1:]
		}
	}
}

// Run cmd and then start a new goroutine to watch the cmd.
// Returns a watchCh for goroutine completion.
func (c *QueueController) runAndWatch(cmdID cmdAndID) chan runner.RunStatus {
	log.Infof("Running: jobID:%s taskID=%s runID=%s, newLen:%d\n",
		cmdID.cmd.JobID, cmdID.cmd.TaskID, cmdID.id, len(c.queue))
	watchCh := make(chan runner.RunStatus)
	abortCh, statusUpdateCh := c.inv.Run(cmdID.cmd, cmdID.id)
	c.runningAbort = abortCh
	c.runningID = cmdID.id
	c.runningCmd = cmdID.cmd
	go func() {
		for st := range statusUpdateCh {
			log.Debugf("Queue pulled result:%+v, jobID:%s taskID=%s runID=%s\n",
				cmdID.cmd.JobID, cmdID.cmd.TaskID, cmdID.id, st)
			c.statusManager.Update(st)
			if st.State.IsDone() {
				watchCh <- st
				return
			}
		}
	}()
	return watchCh
}
