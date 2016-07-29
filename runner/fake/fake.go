package fake

import (
	"github.com/scootdev/scoot/runner"
)

func NewRunner() runner.Runner {
	return &fakeRunner{}
}

type fakeRunner struct {
}

func (r *fakeRunner) Run(cmd *runner.Command) runner.ProcessStatus {
	return success(runner.RunId("1"))
}

func (r *fakeRunner) Status(run runner.RunId) runner.ProcessStatus {
	return success(run)
}

func (r *fakeRunner) StatusAll() []runner.ProcessStatus {
	return []runner.ProcessStatus{success(runner.RunId("1"))}
}

func (r *fakeRunner) Abort(run runner.RunId) runner.ProcessStatus {
	return runner.ProcessStatus{State: runner.ABORTED}
}

func (r *fakeRunner) Erase(run runner.RunId) {
}

func success(runId runner.RunId) runner.ProcessStatus {
	return runner.ProcessStatus{
		RunId:     runId,
		State:     runner.COMPLETE,
		StdoutRef: "/dev/null",
		StderrRef: "/dev/null",
		ExitCode:  0,
		Error:     "",
	}
}
