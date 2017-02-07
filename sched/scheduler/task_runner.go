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

type taskRunner struct {
	saga   *saga.Saga
	runner runner.Service
	stat   stats.StatsReceiver

	markCompleteOnFailure bool
	defaultTaskTimeout    time.Duration
	runnerOverhead        time.Duration

	taskId string
	task   sched.TaskDefinition
}

// Run the task on the specified worker, and update the SagaLog appropriately.  Returns an error if one
// occurs while running a task or writing to the SagaLog.  This method blocks until all saga messages
// are logged and the task completes
// parameters:
func (r *taskRunner) run() error {
	log.Println("Starting task", r.taskId, " command:", strings.Join(r.task.Argv, " "))
	// Log StartTask Message to SagaLog
	err := r.saga.StartTask(r.taskId, nil)
	if err != nil {
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

	err = r.logTaskStatus(st, saga.EndTask)
	if err == nil {
		r.stat.Counter("completedTaskCounter").Inc(1)
	} else {
		r.stat.Counter("failedTaskSagaCounter").Inc(1)
	}
	return err
}

func (r *taskRunner) runAndWait(taskId string, task sched.TaskDefinition) (runner.RunStatus, error) {
	cmd := task.Command
	if cmd.Timeout == 0 {
		cmd.Timeout = r.defaultTaskTimeout
	}

	endTime := time.Now().Add(cmd.Timeout).Add(r.runnerOverhead)

	st, err := r.runner.Run(&cmd)
	if err != nil || st.State.IsDone() {
		return st, err
	}

	id := st.RunID

	st, err = r.queryWithTimeout(id, endTime, true)
	if err != nil || st.State.IsDone() {
		return st, err
	}

	r.logTaskStatus(st, saga.StartTask)

	return r.queryWithTimeout(id, endTime, false)
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

func (r *taskRunner) logTaskStatus(st runner.RunStatus, msgType saga.SagaMessageType) error {
	log.Println("Finishing", st)
	statusAsBytes, err := workerapi.SerializeProcessStatus(st)
	log.Println("Hm", err, statusAsBytes)
	if err != nil {
		r.stat.Counter("failedTaskSerializeCounter").Inc(1)
		return err
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
