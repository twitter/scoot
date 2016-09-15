package workers

import (
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
	status, err := r.runner.Run(&task.Command)
	if err != nil {
		return status, err
	}
	id := status.RunId
	for {
		status, err = r.runner.Status(id)
		if err != nil || status.State.IsDone() {
			return status, err
		}
		time.Sleep(r.period)
	}
}
