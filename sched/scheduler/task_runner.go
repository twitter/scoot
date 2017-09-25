package scheduler

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/workerapi"
)

const DeadLetterTrailer = " -> Error(s) encountered, canceling task."

func emptyStatusError(jobId string, taskId string, err error) string {
	return fmt.Sprintf("Empty run status, jobId: %s, taskId: %s, err: %s", jobId, taskId, err)
}

type taskRunner struct {
	saga   *saga.Saga
	runner runner.Service
	stat   stats.StatsReceiver

	markCompleteOnFailure bool
	taskTimeoutOverhead   time.Duration // How long to wait for a response after the task has timed out.
	defaultTaskTimeout    time.Duration // Use this timeout as the default for any cmds that don't have one.
	runnerRetryTimeout    time.Duration // How long to keep retrying a runner req
	runnerRetryInterval   time.Duration // How long to sleep between runner req retries.

	tags.LogTags
	task   sched.TaskDefinition
	nodeSt *nodeState

	abortCh      chan bool        // Primary channel to check for aborts
	queryAbortCh chan interface{} // Secondary channel to pass to blocking query.

	startTime time.Time
}

// Return a custom error from run() so the scheduler has more context.
type taskError struct {
	sagaErr   error
	runnerErr error
	resultErr error // Note: resultErr is the error from trying to get the results of the command, not an error from the command
	st        runner.RunStatus
}

func (t *taskError) Error() string {
	return fmt.Sprintf("TaskError: saga: %v ### runner: %v ### result: %v", t.sagaErr, t.runnerErr, t.resultErr)
}

// Run the task on the specified worker, and update the SagaLog appropriately.  Returns an error if an
// error occurs trying to run the task, getting the task results or writing to SagaLog.  (Note: if the
// task's command errors when the command is run, this is not considered to be an error.)

// This method blocks until all saga messages are logged and the task completes
func (r *taskRunner) run() error {
	log.WithFields(
		log.Fields{
			"jobID":  r.JobID,
			"taskID": r.TaskID,
			"node":   r.nodeSt.node,
			"task":   r.task,
			"tag":    r.Tag,
		}).Info("Starting task")
	taskErr := &taskError{}

	// Log StartTask Message to SagaLog
	if err := r.logTaskStatus(nil, saga.StartTask); err != nil {
		taskErr.sagaErr = err
		r.stat.Counter(stats.SchedFailedTaskCounter).Inc(1)
		return taskErr
	}

	// Run and update taskErr with the results.
	st, end, err := r.runAndWait()
	taskErr.runnerErr = err
	taskErr.st = st

	// We got a good message back, but it indicates an error. Update taskErr accordingly.
	completed := (st.State == runner.COMPLETE)
	if err == nil && !completed {
		switch st.State {
		case runner.FAILED, runner.UNKNOWN, runner.BADREQUEST:
			// runnerErr can be thrift related above, or in this case some other failure that's likely our fault.
			err = fmt.Errorf(st.Error)
			taskErr.runnerErr = err
		default:
			// resultErr can be (ABORTED,TIMEDOUT), which indicates a transient or user-related concern.
			err = fmt.Errorf(st.State.String())
			taskErr.resultErr = err
		}
	}

	// We should write to sagalog if there's no error, or there's an error but the caller won't be retrying.
	shouldDeadLetter := (err != nil && (end || r.markCompleteOnFailure))
	shouldLog := (err == nil) || shouldDeadLetter

	// Update taskErr state if it's empty or if we're doing deadletter..
	if taskErr.st.State == runner.UNKNOWN {
		taskErr.st.State = runner.FAILED
		taskErr.st.Error = emptyStatusError(r.JobID, r.TaskID, err)
	}
	if shouldDeadLetter {
		log.WithFields(
			log.Fields{
				"jobID":  r.JobID,
				"taskID": r.TaskID,
				"sagaID": r.saga.GetState().SagaId(),
				"err":    taskErr,
				"tag":    r.Tag,
			}).Info("Error running job, dead lettering task after max retries.")
		taskErr.st.Error += DeadLetterTrailer
	}

	log.WithFields(
		log.Fields{
			"node":       r.nodeSt.node,
			"log":        shouldLog,
			"runID":      taskErr.st.RunID,
			"state":      taskErr.st.State,
			"stdout":     taskErr.st.StdoutRef,
			"stderr":     taskErr.st.StderrRef,
			"snapshotID": taskErr.st.SnapshotID,
			"exitCode":   taskErr.st.ExitCode,
			"error":      taskErr.st.Error,
			"jobID":      taskErr.st.JobID,
			"taskID":     taskErr.st.TaskID,
			"tag":        taskErr.st.Tag,
			"err":        taskErr,
		}).Info("End task")
	if !shouldLog {
		if taskErr != nil {
			r.stat.Counter(stats.SchedFailedTaskCounter).Inc(1)
		}
		return taskErr
	}

	err = r.logTaskStatus(&taskErr.st, saga.EndTask)
	taskErr.sagaErr = err
	if taskErr.sagaErr == nil && taskErr.runnerErr == nil && taskErr.resultErr == nil {
		r.stat.Counter(stats.SchedCompletedTaskCounter).Inc(1)
		return nil
	} else {
		r.stat.Counter(stats.SchedFailedTaskCounter).Inc(1)
		return taskErr
	}
}

// Run cmd and if there's a runner error (ex: thrift) re-run/re-query until completion, retry timeout, or cmd timeout.
func (r *taskRunner) runAndWait() (runner.RunStatus, bool, error) {
	cmd := &r.task.Command
	if cmd.Timeout == 0 {
		cmd.Timeout = r.defaultTaskTimeout
	}
	cmdEndTime := time.Now().Add(cmd.Timeout).Add(r.taskTimeoutOverhead)
	elapsedRetryDuration := time.Duration(0)
	var st runner.RunStatus
	var err error
	var id runner.RunID
	var end bool
	cmd.TaskID = r.TaskID
	cmd.JobID = r.JobID
	// If runner call returns an error then we treat it as an infrastructure error and will repeatedly retry.
	// If runner call returns a result indicating cmd error we fail and return.
	//TODO(jschiller): add a Nonce to Cmd so worker knows what to do if it sees a dup command?
	log.WithFields(
		log.Fields{
			"jobID":  r.JobID,
			"taskID": r.TaskID,
			"tag":    r.Tag,
		}).Info("runAndWait()")

	for {
		// was a job kill request received before we could start the run?
		if aborted, endTask := r.abortRequested(); aborted {
			st = runner.AbortStatus(id, tags.LogTags{JobID: r.JobID, TaskID: r.TaskID})
			log.WithFields(
				log.Fields{
					"jobID":  r.JobID,
					"taskID": r.TaskID,
					"tag":    r.Tag,
				}).Info("The run was aborted by the scheduler before it was sent to a worker")
			return st, endTask, nil
		}

		// send the command to the worker
		st, err = r.runner.Run(cmd)

		// was a job kill request received while starting the run?
		if aborted, endTask := r.abortRequested(); aborted {
			if err == nil { // we should have a status with runId, abort the run
				r.runner.Abort(st.RunID)
			}
			st = runner.AbortStatus(id, tags.LogTags{JobID: r.JobID, TaskID: r.TaskID})
			log.WithFields(
				log.Fields{
					"jobID":  r.JobID,
					"taskID": r.TaskID,
					"tag":    r.Tag,
				}).Info("Initial run attempts aborted by the scheduler")
			return st, endTask, nil
		}

		if err != nil && elapsedRetryDuration+r.runnerRetryInterval < r.runnerRetryTimeout {
			log.WithFields(
				log.Fields{
					"jobID":  r.JobID,
					"taskID": r.TaskID,
					"tag":    r.Tag,
				}).Info("Retrying run()")
			r.stat.Counter(stats.SchedTaskStartRetries).Inc(1)
			time.Sleep(r.runnerRetryInterval)
			elapsedRetryDuration += r.runnerRetryInterval
			continue
		} else if err != nil || st.State.IsDone() {
			return st, false, err
		}
		break
	}
	id = st.RunID

	// Wait for the process to start running, log it, then wait for it to finish.
	elapsedRetryDuration = 0
	includeRunning := true
	for {
		st, end, err = r.queryWithTimeout(id, cmdEndTime, includeRunning)
		elapsed := elapsedRetryDuration + r.runnerRetryInterval
		if (err != nil && elapsed >= r.runnerRetryTimeout) || (err == nil && st.State.IsDone()) {
			if st.State != runner.COMPLETE {
				r.runner.Abort(id)
			}
			break
		} else if err != nil {
			log.WithFields(
				log.Fields{
					"jobID":  r.JobID,
					"taskID": r.TaskID,
					"tag":    r.Tag,
				}).Info("Retrying query")
			time.Sleep(r.runnerRetryInterval)
			elapsedRetryDuration += r.runnerRetryInterval
			continue
		} else if includeRunning {
			// It's running, but not done, so we want to log a second StartTask that includes
			// its status, so a watcher can go investigate. Strictly speaking this is optional
			// in that we've already logged a start task and our only obligation is to log a
			//corresponding end task.
			log.WithFields(
				log.Fields{
					"jobID":     r.JobID,
					"taskID":    r.TaskID,
					"node":      r.nodeSt.node,
					"runStatus": st,
					"tag":       r.Tag,
				}).Debug("Update task")
			r.logTaskStatus(&st, saga.StartTask)
			includeRunning = false
		}
	}

	return st, end, err
}

func (r *taskRunner) queryWithTimeout(id runner.RunID, endTime time.Time, includeRunning bool) (runner.RunStatus, bool, error) {
	// setup the query request
	q := runner.Query{Runs: []runner.RunID{id}, States: runner.DONE_MASK}
	if includeRunning {
		q.States = q.States | runner.RUNNING_MASK
	}
	timeout := endTime.Sub(time.Now())
	if timeout < 0 {
		timeout = 0
	}
	// The semantics of timeout changes here. Before, zero meant use the default, here it means return immediately.
	w := runner.Wait{Timeout: timeout, AbortCh: r.queryAbortCh}
	log.WithFields(
		log.Fields{
			"jobID":   r.JobID,
			"taskID":  r.TaskID,
			"timeout": timeout,
			"tag":     r.Tag,
		}).Infof("Query(includeRunning=%t)", includeRunning)

	// issue a query that blocks till get a response, w's timeout, or abort (from job kill)
	// if the abort request triggers the Query() to return, Query() will put a new
	// abort request on the channel to replace the one it consumed, so we know to send
	// an abort to the runner below
	sts, _, err := r.runner.Query(q, w)

	if aborted, endTask := r.abortRequested(); aborted {
		return runner.AbortStatus(id, tags.LogTags{JobID: r.JobID, TaskID: r.TaskID}), endTask, nil
	}
	if err != nil {
		return runner.RunStatus{}, false, err
	}

	var st runner.RunStatus
	if len(sts) == 1 {
		st = sts[0]
	} else {
		st = runner.RunStatus{
			RunID: id,
			State: runner.TIMEDOUT,
		}
	}
	return st, false, nil
}

func (r *taskRunner) logTaskStatus(st *runner.RunStatus, msgType saga.SagaMessageType) error {
	log.WithFields(
		log.Fields{
			"msgType": msgType,
			"jobID":   r.JobID,
			"taskID":  r.TaskID,
			"tag":     r.Tag,
		}).Info("TryLogTaskStatus")
	var statusAsBytes []byte
	var err error
	if st != nil {
		statusAsBytes, err = workerapi.SerializeProcessStatus(*st)
		if err != nil {
			r.stat.Counter(stats.SchedFailedTaskSerializeCounter).Inc(1) // TODO errata metric - remove if unused
			return err
		}
	}

	switch msgType {
	case saga.StartTask:
		err = r.saga.StartTask(r.TaskID, statusAsBytes)
	case saga.EndTask:
		err = r.saga.EndTask(r.TaskID, statusAsBytes)
	default:
		err = fmt.Errorf("unexpected saga message type: %v", msgType)
	}

	return err
}

func (r *taskRunner) abortRequested() (aborted bool, endTask bool) {
	select {
	case endTask := <-r.abortCh:
		log.WithFields(
			log.Fields{
				"jobID":   r.JobID,
				"taskID":  r.TaskID,
				"node":    r.nodeSt.node,
				"endTask": endTask,
				"tag":     r.Tag,
			}).Info("Abort requested")
		return true, endTask
	default:
		return false, false
	}
}

func (r *taskRunner) Abort(endTask bool) {
	r.abortCh <- endTask
	r.queryAbortCh <- nil
}
