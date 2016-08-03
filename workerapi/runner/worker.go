// Wraps a runner.Runner into a Worker
package runner

import (
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/workerapi"
)

func MakeWorker(r runner.Runner) workerapi.Worker {
	return &runnerWorker{r}
}

type runnerWorker struct {
	r runner.Runner
}

func (w *runnerWorker) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	return w.r.Run(cmd), nil
}

func (w *runnerWorker) Status() (*workerapi.WorkerStatus, error) {
	return &workerapi.WorkerStatus{
		Runs: w.r.StatusAll(),
	}, nil
}

func (w *runnerWorker) Close() error {
	return nil
}

func (w *runnerWorker) Abort(run runner.RunId) (runner.ProcessStatus, error) {
	return w.r.Abort(run), nil
}

func (w *runnerWorker) Erase(run runner.RunId) error {
	w.r.Erase(run)
	return nil
}
