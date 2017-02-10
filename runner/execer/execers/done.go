package execers

import "github.com/scootdev/scoot/runner/execer"

// Creates a new doneExecer.
func NewDoneExecer() *DoneExecer {
	return &DoneExecer{
		State: execer.COMPLETE,
	}
}

// doneExecer finishes something as soon as its run
type DoneExecer struct {
	State execer.ProcessState
}

func (e *DoneExecer) Exec(command execer.Command) (execer.Process, error) {
	return e, nil
}

func (e *DoneExecer) completeStatus() execer.ProcessStatus {
	return execer.ProcessStatus{
		State:    e.State,
		ExitCode: 0,
	}
}

func (e *DoneExecer) Wait() execer.ProcessStatus {
	return e.completeStatus()
}

func (e *DoneExecer) Abort() execer.ProcessStatus {
	return e.completeStatus()
}
