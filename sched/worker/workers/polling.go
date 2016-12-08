// Package workers provides Worker implementations that can
// be used to run Tasks. The workers generally differ in how
// they run and manage the underlying Tasks - whether they support
// polling, run timeout enforcement, test and simulated runs, etc.
package workers

// import (
// 	"time"

// 	"github.com/scootdev/scoot/runner"
// 	"github.com/scootdev/scoot/sched"
// 	"github.com/scootdev/scoot/sched/worker"
// )

// // NewPollingWorker creates a PollingWorker
// func NewPollingWorker(
// 	runner runner.Runner,
// 	period time.Duration) worker.Worker {
// 	return NewPollingWorkerWithTimeout(runner, period, false, 0*time.Minute)
// }

// func NewPollingWorker() *

// // Creates a New Polling Worker which enforces a Timeout on Tasks
// func NewPollingWorkerWithTimeout(
// 	service runner.Service,
// 	period time.Duration,
// 	enforceTimout bool,
// 	timeout time.Duration) worker.Worker {

// 		pollingPeriod:  period,
// 		enforceTimeout: enforceTimout,
// 		timeout:        timeout,
// 	return &PollingWorker{
// 		controller: runner,
// 		querier: service,
// 	}
// }

// // PollingWorker acts as a Worker by polling the underlying runner every period
// type PollingWorker struct {
// 	controller runner.Controller
// 	querier    runner.StatusQuerier
// }

// func (r *PollingWorker) RunAndWait(task sched.TaskDefinition) (runner.RunStatus, error) {
// 	// schedule the task
// 	st, err := r.controller.Run(&task.Command)
// 	if err != nil || st.State().IsDone() {
// 		return st, err
// 	}

// 	q := runner.SingleRun(st.RunID)
// 	w := runner.Wait{Timeout: task.Command.Timeout}
// 	stats, err := r.querier.Query(q, w)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if len(stats) == 1 {
// 		return stats[0]
// 	}
// 	return runner.TimeoutStatus(st.RunID)
// }
