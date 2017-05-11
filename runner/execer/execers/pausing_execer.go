package execers

import (
	"fmt"
	"github.com/scootdev/scoot/runner/execer"
	"sync"
)

var PausingExecerMU sync.Mutex

// Creates a new pausingExecer.  It's execution is blocked till Abort() or Resume() is called
// if Resume is called and it is not running, it will return an error indicating it's current
// state (completed or aborted)
func NewPausingExecer() *PausingExecer {
	return &PausingExecer{
		ch: make(chan interface{}),
	}
}

// doneExecer finishes something as soon as its run
type PausingExecer struct {
	ch        chan interface{}
	State     execer.ProcessState
	ExitCode  int
	ExecError error
}

// Exec goes directly to a wait state,  till Resume() or Abort() is called
func (e *PausingExecer) Exec(command execer.Command) (execer.Process, error) {
	e.Wait()
	PausingExecerMU.Lock()
	defer PausingExecerMU.Unlock()
	e.State = execer.COMPLETE
	e.ExitCode = 0
	return e, e.ExecError
}

// pauses processing till Resume() or Abort() is called
func (e *PausingExecer) Wait() execer.ProcessStatus {
	PausingExecerMU.Lock()
	e.State = execer.RUNNING
	PausingExecerMU.Unlock()
	<-e.ch
	return execer.ProcessStatus{
		State:    execer.COMPLETE,
		ExitCode: 0,
	}
}

// allows a prior Exec() or Wait() call to proceed
// the return State is UNKNOWN and ExitCode is 1
func (e *PausingExecer) Abort() execer.ProcessStatus {
	e.ch <- 1
	return execer.ProcessStatus{
		State:    execer.UNKNOWN,
		ExitCode: 1,
	}
}

// allows a prior Exec() or Wait() call to proceed
// the return State is UNKNOWN and ExitCode is 1
func (e *PausingExecer) Resume() error {
	PausingExecerMU.Lock()

	if e.State != execer.RUNNING {
		PausingExecerMU.Unlock()
		return fmt.Errorf("Execer is not running, it's state is: %s", e.State.String())
	}
	PausingExecerMU.Unlock()
	e.ch <- 1

	return nil
}
