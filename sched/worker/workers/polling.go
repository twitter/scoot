package workers

import (
	"fmt"
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/worker"
)

func NewPollingWorker(runner runner.Runner, period time.Duration) worker.Worker {
	return &PollingWorker{runner, period}
}

type PollingWorker struct {
	runner runner.Runner
	period time.Duration
}

func (r *PollingWorker) RunAndWait(task sched.TaskDefinition) (runner.ProcessStatus, error) {
	status := r.runner.Run(&task.Command)
	if status.State == runner.BADREQUEST {
		return runner.ProcessStatus{}, fmt.Errorf("error running task %v: %v", task, status.Error)
	}
	id := status.RunId
	for !status.State.IsDone() {
		status = r.runner.Status(id)
		if status.State == runner.BADREQUEST {
			return runner.ProcessStatus{}, fmt.Errorf("error querying run %v: %v", id, status.Error)
		}
		time.Sleep(r.period)
	}
	return status, nil
}
