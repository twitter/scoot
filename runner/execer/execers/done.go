package execers

import "github.com/twitter/scoot/runner/execer"

// Creates a new doneExecer.
func NewDoneExecer() *DoneExecer {
	return &DoneExecer{
		State: execer.COMPLETE,
	}
}

// doneExecer finishes something as soon as its run
type DoneExecer struct {
	State     execer.ProcessState
	ExitCode  int
	ExecError error
}

func (e *DoneExecer) Exec(command execer.Command) (execer.Process, error) {
	return e, e.ExecError
}

func (e *DoneExecer) completeStatus() execer.ProcessStatus {
	return execer.ProcessStatus{
		State:    e.State,
		ExitCode: e.ExitCode,
	}
}

func (e *DoneExecer) Wait() execer.ProcessStatus {
	return e.completeStatus()
}

func (e *DoneExecer) Abort() execer.ProcessStatus {
	return e.completeStatus()
}
