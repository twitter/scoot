package execers

import (
	"github.com/scootdev/scoot/runner/execer"
)

func NewDoneExecer() execer.Execer {
	return &doneExecer{}
}

type doneExecer struct{}

func (e *doneExecer) Exec(command execer.Command) (execer.Process, error) {
	return e, nil
}

var complete = execer.ProcessStatus{
	State:    execer.COMPLETE,
	ExitCode: 0,
}

func (e *doneExecer) Wait() execer.ProcessStatus {
	return complete
}

func (e *doneExecer) Abort() execer.ProcessStatus {
	return complete
}
