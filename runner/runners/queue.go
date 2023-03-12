package runners

import (
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/wisechengyi/scoot/common/errors"
	"github.com/wisechengyi/scoot/common/log/hooks"
	"github.com/wisechengyi/scoot/common/log/tags"
	"github.com/wisechengyi/scoot/common/stats"
	"github.com/wisechengyi/scoot/runner"
	"github.com/wisechengyi/scoot/runner/execer"
	"github.com/wisechengyi/scoot/snapshot"
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
	} else {
		// setting Error level to avoid Travis test failure due to log too long
		log.SetLevel(log.ErrorLevel)
	}
}

const QueueFullMsg = "No resources available. Please try later."
const QueueInitingMsg = "Queue is still initializing. Please try later."
const QueueInvalidMsg = "Failed initialization, queue permanently broken."
const WorkerUnhealthyMsg = "Worker is unhealthy."

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
@param: filerMap - mapping of runner.RunType's to filers and corresponding InitDoneCh's
that are used by underlying Invokers. The Controller waits on all non-nil InitDoneCh's
to complete successfully before serving requests.
@param: output
@param: tmp
@param: capacity - the maximum number of commands to support on the queue.  If 0 then the queue is unbounded.
@param: stats - the stats receiver the queue will use when reporting its metrics
@param: dirMonitor - monitor directory size changes from running the task's command
@param: rID - the runner id
*/
func NewQueueRunner(
	exec execer.Execer,
	filerMap runner.RunTypeMap,
	output runner.OutputCreator,
	capacity int,
	stat stats.StatsReceiver,
	dirMonitor *stats.DirsMonitor,
	rID runner.RunnerID,
	preprocessors []func() error,
	postprocessors []func() error,
	uploader LogUploader,
) runner.Service {
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
	inv := NewInvoker(exec, filerMap, output, stat, dirMonitor, rID, preprocessors, postprocessors, uploader)

	controller := &QueueController{
		statusManager: statusManager,
		inv:           inv,
		filerMap:      filerMap,
		updateReq:     make(map[runner.RunType]bool),
		capacity:      capacity,
		reqCh:         make(chan interface{}),
		updateCh:      make(chan interface{}),
		cancelTimerCh: make(chan interface{}),
	}
	run := &Service{controller, statusManager}

	// QueueRunner waits on filers with InitDoneChannels defined to return,
	// and will not serve requests if any return an error
	var wg sync.WaitGroup
	var initErr error = nil
	wait := false

	for t, f := range filerMap {
		if f.IDC != nil {
			log.Infof("Starting goroutine to wait on init for filer %v", t)
			wg.Add(1)
			wait = true

			go func(rt runner.RunType, idc snapshot.InitDoneCh) {
				err := <-idc
				if err != nil {
					initErr = err
					log.Errorf("Init channel for filer %v returned error: %s", rt, err)
				}
				wg.Done()
			}(t, f.IDC)
		}
	}

	if wait {
		// Wait for initialization to complete in a new goroutine to let this constructor return
		go func() {
			wg.Wait()
			if initErr != nil {
				stat.Counter(stats.WorkerDownloadInitFailure).Inc(1)
				statusManager.UpdateService(runner.ServiceStatus{Initialized: false, Error: initErr})
			} else {
				statusManager.UpdateService(runner.ServiceStatus{Initialized: true, IsHealthy: true})
				stat.Gauge(stats.WorkerUnhealthy).Update(0)
				controller.startUpdateTickers()
			}
		}()
	} else {
		statusManager.UpdateService(runner.ServiceStatus{Initialized: true, IsHealthy: true})
		stat.Gauge(stats.WorkerUnhealthy).Update(0)
		controller.startUpdateTickers()
	}

	go controller.loop()

	return run
}

// NewSingleRunner create a SingleRunner
func NewSingleRunner(
	exec execer.Execer,
	filerMap runner.RunTypeMap,
	output runner.OutputCreator,
	stat stats.StatsReceiver,
	dirMonitor *stats.DirsMonitor,
	rID runner.RunnerID,
	preprocessors []func() error,
	postprocessors []func() error,
	uploader LogUploader,
) runner.Service {
	return NewQueueRunner(exec, filerMap, output, 0, stat, dirMonitor, rID, preprocessors, postprocessors, uploader)
}

// QueueController maintains a queue of commands to run (up to capacity).
// Manages updates to underlying Filer via Filer's Update interface,
// if a non-zero update interval is defined (updates and tasks cannot run concurrently)
type QueueController struct {
	inv           *Invoker
	filerMap      runner.RunTypeMap
	updateReq     map[runner.RunType]bool
	updateLock    sync.Mutex
	statusManager *StatusManager
	capacity      int

	queue        []cmdAndID
	runningID    runner.RunID
	runningCmd   *runner.Command
	runningAbort chan<- struct{}

	// used to track if there is a recurring infrastructure issue
	lastExitCode errors.ExitCode

	// used to signal a cmd run request
	reqCh chan interface{}
	// used to signal a request to update the Filer
	updateCh chan interface{}
	// used to cancel the timer goroutine if started.
	cancelTimerCh chan interface{}
}

// Start goroutines that trigger periodic filer updates after controller is initialized
func (c *QueueController) startUpdateTickers() {
	if c.updateCh == nil {
		return
	}

	for t, f := range c.filerMap {
		d := f.Filer.UpdateInterval()
		if d == snapshot.NoDuration {
			continue
		}

		go func(rt runner.RunType, td time.Duration) {
			ticker := time.NewTicker(td)
		Loop:
			for {
				select {
				case <-ticker.C:
					c.updateLock.Lock()
					c.updateReq[rt] = true
					c.updateLock.Unlock()

					c.updateCh <- nil
				case <-c.cancelTimerCh:
					ticker.Stop()
					break Loop
				}
			}
		}(t, d)
	}
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
			"ready":          svcStatus.Initialized && svcStatus.IsHealthy,
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
	if !svcStatus.IsHealthy {
		errStr := WorkerUnhealthyMsg
		if svcStatus.Error != nil {
			errStr = svcStatus.Error.Error()
		}
		return runner.RunStatus{Error: errStr}, fmt.Errorf(WorkerUnhealthyMsg)
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

	var idleLatency = c.inv.stat.Latency(stats.WorkerIdleLatency_ms)
	idleLatency.Time()

	tryUpdate := func() {
		if watchCh == nil && updateDoneCh == nil {
			updateRequested = false
			updateDoneCh = make(chan interface{})
			go func() {
				// Record filers by RunType that have requested updates
				c.updateLock.Lock()
				typesToUpdate := []runner.RunType{}
				for t, u := range c.updateReq {
					if u {
						typesToUpdate = append(typesToUpdate, t)
						c.updateReq[t] = false
					}
				}
				c.updateLock.Unlock()

				// Run all requested filer updates serially
				for _, t := range typesToUpdate {
					log.Infof("Running filer update for type %v", t)
					if err := c.filerMap[t].Filer.Update(); err != nil {
						fields := log.Fields{"err": err}
						if c.runningCmd != nil {
							fields["runType"] = t
							fields["tag"] = c.runningCmd.Tag
							fields["jobID"] = c.runningCmd.JobID
							fields["taskID"] = c.runningCmd.TaskID
						}
						log.WithFields(fields).Error("Error running Filer Update")
					}
				}
				updateDoneCh <- nil
			}()
		} else {
			updateRequested = true
		}
	}

	tryRun := func() {
		if watchCh == nil && updateDoneCh == nil && len(c.queue) > 0 && (c.statusManager.svcStatus.IsHealthy) {
			cmdID := c.queue[0]
			watchCh = c.runAndWatch(cmdID)
			idleLatency.Stop()
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
				close(c.cancelTimerCh)
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
			idleLatency.Time()
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
				c.checkAndUpdateServiceHealth(st.ExitCode)
				c.lastExitCode = errors.ExitCode(st.ExitCode)
				watchCh <- st
				return
			}
		}
	}()
	return watchCh
}

/*
checkAndUpdateServiceHealth updates service status to unhealthy if critical or persistent errors occurred
*/
func (c *QueueController) checkAndUpdateServiceHealth(exitCode errors.ExitCode) {
	var svcStatusErr error
	markUnhealthy := false
	if c.isCriticalError(exitCode) {
		svcStatusErr = fmt.Errorf("critical error (%d) occurred. Marking worker unhealthy", exitCode)
		markUnhealthy = true
	}
	if c.isPersistentError(exitCode) {
		svcStatusErr = fmt.Errorf("errors (%d) occurred multiple times in a row. Marking worker unhealthy", exitCode)
		markUnhealthy = true
	}
	if markUnhealthy {
		svcStatus := c.statusManager.svcStatus
		svcStatus.IsHealthy = false
		svcStatus.Error = svcStatusErr
		c.statusManager.UpdateService(svcStatus)
		c.inv.stat.Gauge(stats.WorkerUnhealthy).Update(1)
	}
}

/*
isPersistentError returns true when one of the critical errors has occurred 2 times in a row.
*/
func (c *QueueController) isPersistentError(exitCode errors.ExitCode) bool {
	return exitCode == c.lastExitCode &&
		(exitCode == errors.CleanFailureExitCode ||
			exitCode == errors.CheckoutFailureExitCode)
}

/*
isCriticalError returns true when one of the critical errors has occurred.
*/
func (c *QueueController) isCriticalError(exitCode errors.ExitCode) bool {
	return exitCode == errors.HighInitialMemoryUtilizationExitCode
}
