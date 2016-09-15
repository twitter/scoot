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

var completeStatus = execer.ProcessStatus{
	State:    execer.COMPLETE,
	ExitCode: 0,
}

func (e *doneExecer) Wait() execer.ProcessStatus {
	return completeStatus
}

func (e *doneExecer) Abort() execer.ProcessStatus {
	return completeStatus
}
