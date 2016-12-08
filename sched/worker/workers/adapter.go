// Package workers provides Worker implementations that can
// be used to run Tasks. The workers generally differ in how
// they run and manage the underlying Tasks - whether they support
// polling, run timeout enforcement, test and simulated runs, etc.
package workers

import (
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched"
)

func NewServiceWorker(del runner.Service, timeout time.Duration, overhead time.Duration) *ServiceWorkerAdapter {
	return &ServiceWorkerAdapter{del, timeout, overhead}
}

type ServiceWorkerAdapter struct {
	del      runner.Service
	timeout  time.Duration
	overhead time.Duration
}

func (a *ServiceWorkerAdapter) RunAndWait(task sched.TaskDefinition) (runner.RunStatus, error) {
	cmd := task.Command
	if cmd.Timeout == 0 {
		cmd.Timeout = a.timeout
	}
	st, err := a.del.Run(&cmd)
	if err != nil || st.State.IsDone() {
		return st, err
	}

	id := st.RunID

	q := runner.Query{Runs: []runner.RunID{id}, States: runner.DONE_MASK}
	w := runner.Wait{Timeout: cmd.Timeout + a.overhead}
	stats, err := a.del.Query(q, w)
	if err != nil {
		return runner.RunStatus{}, err
	}
	if len(stats) == 1 {
		return stats[0], nil
	}
	return runner.TimeoutStatus(st.RunID), nil
}
