package fake

import (
	"github.com/scootdev/scoot/runner"
)

func NewRunner() runner.Runner {
	return &fakeRunner{}
}

type fakeRunner struct {
}

func (r *fakeRunner) Run(cmd *runner.Command) (*runner.ProcessStatus, error) {
	return success(runner.RunId("1")), nil
}

func (r *fakeRunner) Status(run runner.RunId) (*runner.ProcessStatus, error) {
	return success(run), nil
}

func success(runId runner.RunId) *runner.ProcessStatus {
	return &runner.ProcessStatus{
		RunId:     runId,
		State:     runner.COMPLETED,
		StdoutRef: "/dev/null",
		StderrRef: "/dev/null",
		ExitCode:  0,
		Error:     "",
	}
}
