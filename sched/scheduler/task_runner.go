package scheduler

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/workerapi"
)

const DeadLetterExitCode = -200

func emptyStatusError(jobId string, taskId string, err error) string {
	return fmt.Sprintf("Empty run status, jobId: %s, taskId: %s, err: %s", jobId, taskId, err)
}

type taskRunner struct {
	saga   *saga.Saga
	runner runner.Service
	stat   stats.StatsReceiver

	markCompleteOnFailure bool
	defaultTaskTimeout    time.Duration // Use this timeout as the default for any cmds that don't have one.
	runnerRetryTimeout    time.Duration // How long to keep retrying a runner req
	runnerRetryInterval   time.Duration // How long to sleep between runner req retries.
	runnerOverhead        time.Duration // Runner will timeout after the caller-provided timeout plus this overhead.

	jobId  string
	taskId string
	task   sched.TaskDefinition
	nodeId cluster.NodeId

	abortCh chan bool
}

// Return a custom error from run() so the scheduler has more context.
type taskError struct {
	sagaErr   error
	runnerErr error
	resultErr error
	st        runner.RunStatus
}

func (t *taskError) Error() string {
	return fmt.Sprintf("TaskError: saga: %v ### runner: %v ### result: %v", t.sagaErr, t.runnerErr, t.resultErr)
}

// Run the task on the specified worker, and update the SagaLog appropriately.  Returns an error if one
// occurs while running a task or writing to the SagaLog.  This method blocks until all saga messages
// are logged and the task completes
// parameters:
func (r *taskRunner) run() error {
	log.Infof("Starting task - jobId: %s, taskId: %s, node: %s -> %v", r.jobId, r.taskId, r.nodeId, r.task)
	taskErr := &taskError{}

	// Log StartTask Message to SagaLog
	if err := r.logTaskStatus(nil, saga.StartTask); err != nil {
		taskErr.sagaErr = err
		return taskErr
	}

	// Run and update taskErr with the results.
	st, err := r.runAndWait(r.taskId, r.task)
	taskErr.runnerErr = err
	taskErr.st = st

	// We got a good message back, but it indicates an error. Update taskErr accordingly.
	if err == nil && st.State != runner.COMPLETE {
		switch st.State {
		case runner.FAILED:
			err = fmt.Errorf(st.Error)
		default:
			err = fmt.Errorf(st.State.String())
		}
		taskErr.resultErr = err
	}

	// We should write to sagalog if there's no error, or there's an error but the caller won't be retrying.
	shouldDeadLetter := (err != nil && r.markCompleteOnFailure)
	shouldLog := (err == nil) || shouldDeadLetter

	// Update taskErr state if it's empty or if we're doing deadletter..
	if taskErr.st.State == runner.UNKNOWN {
		taskErr.st.State = runner.FAILED
		taskErr.st.Error = emptyStatusError(r.jobId, r.taskId, err)
	}
	if shouldDeadLetter {
		taskErr.st.ExitCode = DeadLetterExitCode
		log.Infof(
			`Error Running jobId: %s, taskId: %s: dead lettering task after max retries. sagaId: %s, Error: %v`,
			r.jobId, r.taskId, r.saga.GetState().SagaId(), taskErr)
	}

	log.Infof("End task - jobId: %s, taskId: %s, node: %s, log: %t, runStatus: %s, err: %v",
		r.jobId, r.taskId, r.nodeId, shouldLog, taskErr.st, taskErr)
	if !shouldLog {
		return taskErr
	}

	err = r.logTaskStatus(&taskErr.st, saga.EndTask)
	taskErr.sagaErr = err
	if taskErr.sagaErr == nil && taskErr.runnerErr == nil && taskErr.resultErr == nil {
		r.stat.Counter("completedTaskCounter").Inc(1)
		return nil
	} else {
		r.stat.Counter("failedTaskSagaCounter").Inc(1)
		return taskErr
	}
}

// Run cmd and if there's a runner error (ex: thrift) re-run/re-query until completion, retry timeout, or cmd timeout.
func (r *taskRunner) runAndWait(taskId string, task sched.TaskDefinition) (runner.RunStatus, error) {
	cmd := &task.Command
	if cmd.Timeout == 0 {
		cmd.Timeout = r.defaultTaskTimeout
	}
	cmdEndTime := time.Now().Add(cmd.Timeout).Add(r.runnerOverhead)
	elapsedRetryDuration := time.Duration(0)
	var st runner.RunStatus
	var err error
	var id runner.RunID
	cmd.TaskID = r.taskId
	cmd.JobID = r.jobId
	// If runner call returns an error then we treat it as an infrastructure error and will repeatedly retry.
	// If runner call returns a result indicating cmd error we fail and return.
	//TODO(jschiller): add a Nonce to Cmd so worker knows what to do if it sees a dup command?
	log.Infof("Run() for jobId: %s taskId: %s", r.jobId, taskId)
	for {
		// was a job kill request received before we could start the run?
		if r.abortRequested() {
			st = runner.AbortStatus(id, runner.LogTags{JobID: r.jobId, TaskID: r.taskId})
			log.Infof("The run was aborted by scheduler before it was sent to worker: jobId: %s taskId: %s", r.jobId, taskId)
			return st, nil
		}

		// send the command to the worker
		st, err = r.runner.Run(cmd)

		// was a job kill request received while startingt the run?
		if r.abortRequested() {
			if err == nil { // we should have a status with runId, abort the run
				r.runner.Abort(st.RunID)
			}
			st = runner.AbortStatus(id, runner.LogTags{JobID: r.jobId, TaskID: r.taskId})
			log.Infof("Initial run attempts aborted by scheduler : jobId: %s taskId: %s", r.jobId, taskId)
			err = nil
		}

		if err != nil && elapsedRetryDuration+r.runnerRetryInterval < r.runnerRetryTimeout {
			log.Infof("Retrying run() for jobId: %s taskId: %s", r.jobId, taskId)
			time.Sleep(r.runnerRetryInterval)
			elapsedRetryDuration += r.runnerRetryInterval
			continue
		} else if err != nil || st.State.IsDone() {
			return st, err
		}
		break
	}
	id = st.RunID

	log.Infof("Query(running) for jobId: %s, taskId: %s", r.jobId, taskId)
	// Wait for the process to start running, log it, then wait for it to finish.
	elapsedRetryDuration = 0
	includeRunning := true
	for {
		st, err = r.queryWithTimeout(id, cmdEndTime, includeRunning)
		elapsed := elapsedRetryDuration + r.runnerRetryInterval
		if (err != nil && elapsed >= r.runnerRetryTimeout) || st.State.IsDone() {
			break
		} else if err != nil {
			log.Infof("Retrying query(includeRunning=%t) for jobId: %s, taskId: %s", includeRunning, r.jobId, taskId)
			time.Sleep(r.runnerRetryInterval)
			elapsedRetryDuration += r.runnerRetryInterval
			continue
		} else if includeRunning {
			// It's running, but not done, so we want to log a second StartTask that includes
			// its status, so a watcher can go investigate. Strictly speaking this is optional
			// in that we've already logged a start task and our only obligation is to log a
			//corresponding end task.
			r.logTaskStatus(&st, saga.StartTask)
			includeRunning = false
		}
	}

	return st, err
}

func (r *taskRunner) queryWithTimeout(id runner.RunID, endTime time.Time, includeRunning bool) (runner.RunStatus, error) {
	// setup the query request
	q := runner.Query{Runs: []runner.RunID{id}, States: runner.DONE_MASK}
	if includeRunning {
		q.States = q.States | runner.RUNNING_MASK
	}
	timeout := endTime.Sub(time.Now())
	if timeout < 0 {
		timeout = 0
	}
	w := runner.Wait{Timeout: timeout, AbortCh: r.abortCh}

	// issue a query that blocks till get a response, w's timeout, or abort (from job kill)
	// if the abort request triggers the Query() to return, Query() will put a new
	// abort request on the channel to replace the one it consumed, so we know to send
	// an abort to the runner below
	sts, _, err := r.runner.Query(q, w)

	if err != nil {
		return runner.RunStatus{}, err
	}

	if r.abortRequested() {
		r.runner.Abort(id)
		return runner.AbortStatus(id, runner.LogTags{JobID: r.jobId, TaskID: r.taskId}), nil
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
	return st, nil
}

func (r *taskRunner) logTaskStatus(st *runner.RunStatus, msgType saga.SagaMessageType) error {
	var statusAsBytes []byte
	var err error
	if st != nil {
		statusAsBytes, err = workerapi.SerializeProcessStatus(*st)
		if err != nil {
			r.stat.Counter("failedTaskSerializeCounter").Inc(1)
			return err
		}
	}

	switch msgType {
	case saga.StartTask:
		err = r.saga.StartTask(r.taskId, statusAsBytes)
	case saga.EndTask:
		err = r.saga.EndTask(r.taskId, statusAsBytes)
	default:
		err = fmt.Errorf("unexpected saga message type: %v", msgType)
	}

	return err
}

func (r *taskRunner) abortRequested() bool {
	select {
	case <-r.abortCh:
		return true
	default:
		return false
	}
}
