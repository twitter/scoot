package scheduler

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/workerapi"
)

const DeadLetterExitCode = -200
const DefaultRetryInterval = time.Second

type taskRunner struct {
	saga   *saga.Saga
	runner runner.Service
	stat   stats.StatsReceiver

	markCompleteOnFailure bool
	defaultTaskTimeout    time.Duration
	thriftRetryTimeout    time.Duration
	runnerOverhead        time.Duration

	jobId  string
	taskId string
	task   sched.TaskDefinition
}

// Run the task on the specified worker, and update the SagaLog appropriately.  Returns an error if one
// occurs while running a task or writing to the SagaLog.  This method blocks until all saga messages
// are logged and the task completes
// parameters:
func (r *taskRunner) run() error {
	log.Println("Starting task - job:", r.jobId, " task:", r.taskId, " command:", strings.Join(r.task.Argv, " "))
	// Log StartTask Message to SagaLog
	if err := r.logTaskStatus(nil, saga.StartTask); err != nil {
		return err
	}

	st, err := r.runAndWait(r.taskId, r.task)

	if err == nil && st.State != runner.COMPLETE {
		// we got a good message back, but the message is that an error occured
		switch st.State {
		case runner.FAILED:
			err = fmt.Errorf(st.Error)
		default:
			err = fmt.Errorf(st.State.String())
		}
	}

	log.Printf("End task - job:%s, task:%s, runStatus:%s\n", r.jobId, r.taskId, st.String())

	shouldLog := (err == nil)

	if err != nil && r.markCompleteOnFailure {
		st.Error = err.Error()
		st.ExitCode = DeadLetterExitCode
		log.Printf(
			`Error Running Task %v: dead lettering task after max retries.
				TaskDef: %+v, Saga Id: %v, Error: %v`,
			r.taskId, r.task, r.saga.GetState().SagaId(), err)
		shouldLog = true
	}
	if !shouldLog {
		return err
	}

	err = r.logTaskStatus(&st, saga.EndTask)
	if err == nil {
		r.stat.Counter("completedTaskCounter").Inc(1)
	} else {
		r.stat.Counter("failedTaskSagaCounter").Inc(1)
	}
	return err
}

// Run cmd and if there's a thrift error try re-running/re-querying until completion, retry timeout, or cmd timeout.
func (r *taskRunner) runAndWait(taskId string, task sched.TaskDefinition) (runner.RunStatus, error) {
	cmd := task.Command
	if cmd.Timeout == 0 {
		cmd.Timeout = r.defaultTaskTimeout
	}
	cmdEndTime := time.Now().Add(cmd.Timeout).Add(r.runnerOverhead)
	elapsedRetryTime := time.Duration(0)
	var st runner.RunStatus
	var err error
	var id runner.RunID

	// If thrift call returns an error then we treat it is a thrift error and will repeatedly retry.
	// If thrift call returns a domain error that wasn't handled by the runner we should fail/return.
	//TODO(jschiller): add a Nonce to Cmd so worker knows what to do if it sees a dup command?
	for {
		st, err = r.runner.Run(&cmd)
		if err != nil && elapsedRetryTime+DefaultRetryInterval < r.thriftRetryTimeout {
			time.Sleep(DefaultRetryInterval)
			elapsedRetryTime += DefaultRetryInterval
			continue
		} else if err != nil || st.State.IsDone() {
			return st, err
		}
		break
	}
	id = st.RunID

	// Wait for the process to start running
	elapsedRetryTime = 0
	for {
		st, err = r.queryWithTimeout(id, cmdEndTime, true)
		if err != nil && elapsedRetryTime+DefaultRetryInterval < r.thriftRetryTimeout {
			time.Sleep(DefaultRetryInterval)
			elapsedRetryTime += DefaultRetryInterval
			continue
		} else if err != nil || st.State.IsDone() {
			return st, err
		}
		break
	}

	// It's running, but not done, so we want to log a second StartTask that includes
	// its status, so a watcher can go investigate
	r.logTaskStatus(&st, saga.StartTask)

	// Wait for the task to finish or timeout.
	elapsedRetryTime = 0
	for {
		st, err = r.queryWithTimeout(id, cmdEndTime, false)
		if err != nil && elapsedRetryTime+DefaultRetryInterval < r.thriftRetryTimeout {
			time.Sleep(DefaultRetryInterval)
			elapsedRetryTime += DefaultRetryInterval
			continue
		}
		break
	}
	return st, err
}

func (r *taskRunner) queryWithTimeout(id runner.RunID, endTime time.Time, includeRunning bool) (runner.RunStatus, error) {
	q := runner.Query{Runs: []runner.RunID{id}, States: runner.DONE_MASK}
	if includeRunning {
		q.States = q.States | runner.RUNNING_MASK
	}
	timeout := endTime.Sub(time.Now())
	if timeout < 0 {
		timeout = 0
	}
	w := runner.Wait{Timeout: timeout}
	sts, err := r.runner.Query(q, w)
	if err != nil {
		return runner.RunStatus{}, err
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
