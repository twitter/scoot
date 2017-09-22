package runners

import (
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/snapshot"
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
	exec execer.Execer, filer snapshot.Filer, idc snapshot.InitDoneCh, output runner.OutputCreator, tmp *temp.TempDir, capacity int, stat stats.StatsReceiver) runner.Service {

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
		updateCh:      make(chan interface{}),
		cancelTimerCh: make(chan interface{}, 1),
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
			} else {
				statusManager.UpdateService(runner.ServiceStatus{Initialized: true})
				startUpdateTicker(filer.UpdateInterval(), controller.updateCh, controller.cancelTimerCh)
			}
		}()
	} else {
		statusManager.UpdateService(runner.ServiceStatus{Initialized: true})
		startUpdateTicker(filer.UpdateInterval(), controller.updateCh, controller.cancelTimerCh)
	}

	go controller.loop()

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

	queue        []cmdAndID
	runningID    runner.RunID
	runningCmd   *runner.Command
	runningAbort chan<- struct{}

	// used to signal a cmd run request
	reqCh chan interface{}
	// used to signal a request to update the Filer
	updateCh chan interface{}
	// used to cancel the timer goroutine if started.
	cancelTimerCh chan interface{}
}

// Start a goroutine that forwards update ticks to updateCh, so that we can skip if e.g. init failed
func startUpdateTicker(d time.Duration, u chan<- interface{}, cancelCh <-chan interface{}) {
	if d == snapshot.NoDuration || u == nil {
		return
	}
	ticker := time.NewTicker(d)
	go func() {
	Loop:
		for {
			select {
			case <-ticker.C:
				u <- nil
			case <-cancelCh:
				ticker.Stop()
				break Loop
			}
		}
	}()
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
	log.WithFields(
		log.Fields{
			"ready":          svcStatus.Initialized,
			"err":            svcStatus.Error,
			"availableSlots": c.capacity - len(c.queue),
			"totalSlots":     c.capacity,
			"currentRun":     c.runningID,
			"jobID":          cmd.JobID,
			"taskID":         cmd.TaskID,
			"tag":            cmd.Tag,
		}).Info("Trying to run")

	if !svcStatus.Initialized {
		errStr := QueueInitingMsg
		if svcStatus.Error != nil {
			errStr = svcStatus.Error.Error()
		}
		return runner.RunStatus{Error: errStr}, fmt.Errorf(QueueInitingMsg)
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
			log.WithFields(
				log.Fields{
					"currentRun": c.runningID,
					"jobID":      c.runningCmd.JobID,
					"taskID":     c.runningCmd.TaskID,
					"tag":        c.runningCmd.Tag,
				}).Info("Aborting")
			close(c.runningAbort)
			c.runningAbort = nil
		}
	} else {
		for i, cmdID := range c.queue {
			if run == cmdID.id {
				log.WithFields(
					log.Fields{
						"run":    run,
						"jobID":  cmdID.cmd.JobID,
						"taskID": cmdID.cmd.TaskID,
						"tag":    cmdID.cmd.Tag,
					}).Info("Aborting queued run")
				c.queue = append(c.queue[:i], c.queue[i+1:]...)
				c.statusManager.Update(runner.AbortStatus(
					run,
					tags.LogTags{
						JobID:  cmdID.cmd.JobID,
						TaskID: cmdID.cmd.TaskID,
						Tag:    cmdID.cmd.Tag,
					},
				))
			}
		}
	}

	status, _, err := runner.FinalStatus(c.statusManager, run)
	return status, err
}

// Cancels all goroutines created by this instance and exits run loop.
func (c *QueueController) Release() {
	close(c.reqCh)
}

// Handle requests to run and update, to provide concurrency management between the two.
// Although we can still receive run requests, runs and updates are done blocking.
func (c *QueueController) loop() {
	var watchCh chan runner.RunStatus
	var updateDoneCh chan interface{}
	updateRequested := false

	tryUpdate := func() {
		if watchCh == nil && updateDoneCh == nil {
			updateRequested = false
			updateDoneCh = make(chan interface{})
			go func() {
				if err := c.filer.Update(); err != nil {
					log.WithFields(
						log.Fields{
							"err":    err,
							"tag":    c.runningCmd.Tag,
							"jobID":  c.runningCmd.JobID,
							"taskID": c.runningCmd.TaskID,
						}).Error("error running Filer Update")
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
				c.cancelTimerCh <- nil
				return
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
	log.WithFields(
		log.Fields{
			"jobID":  cmdID.cmd.JobID,
			"taskID": cmdID.cmd.TaskID,
			"runID":  cmdID.id,
			"newLen": len(c.queue),
			"tag":    cmdID.cmd.Tag,
		}).Info("Running")
	watchCh := make(chan runner.RunStatus)
	abortCh, statusUpdateCh := c.inv.Run(cmdID.cmd, cmdID.id)
	c.runningAbort = abortCh
	c.runningID = cmdID.id
	c.runningCmd = cmdID.cmd
	go func() {
		for st := range statusUpdateCh {
			log.WithFields(
				log.Fields{
					"runID":      st.RunID,
					"state":      st.State,
					"stdout":     st.StdoutRef,
					"stderr":     st.StderrRef,
					"snapshotID": st.SnapshotID,
					"exitCode":   st.ExitCode,
					"error":      st.Error,
					"jobID":      st.JobID,
					"taskID":     st.TaskID,
					"tag":        st.Tag,
				}).Info("Queue received status update")
			c.statusManager.Update(st)
			if st.State.IsDone() {
				watchCh <- st
				return
			}
		}
	}()
	return watchCh
}
