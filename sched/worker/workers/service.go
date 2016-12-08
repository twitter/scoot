// Package workers provides Worker implementations that can
// be used to run Tasks. The workers generally differ in how
// they run and manage the underlying Tasks - whether they support
// polling, run timeout enforcement, test and simulated runs, etc.
package workers

import (
	"time"

	"github.com/scootdev/scoot/runner"
)

func NewServiceWorker(del runner.Service, overhead time.Duration) *ServiceWorkerAdapter {
	return &ServiceWorkerAdapter{del, overhead}
}

type ServiceWorkerAdapter struct {
	del      runner.Service
	overhead time.Duration
}

func (a *ServiceWorkerAdapter) RunAndWait(task sched.TaskDefinition) (runner.RunStatus, error) {
	cmd := task.Command
	st, err := a.del.Run(&cmd)
	if err != nil || st.State().IsDone() {
		return st, err
	}

	q := runner.SingleRun(st.RunID)
	w := runner.Wait{Timeout: task.Command.Timeout.Add(a.overhead)}
	stats, err := r.querier.Query(q, w)
	if err != nil {
		return nil, err
	}
	if len(stats) == 1 {
		return stats[0]
	}
	return runner.TimeoutStatus(st.RunID)
}
