package execers

import (
	"testing"
	"github.com/scootdev/scoot/runner/execer"
	"time"
)

func TestPausingExecer(t *testing.T) {
	ex := NewPausingExecer()

	go func(ex *PausingExecer) {
		ex.Exec(execer.Command{})
	}(ex)

	// wait till it starts running
	waitForStatus(ex, execer.RUNNING)


	err := ex.Resume()
	if err != nil {
		t.Fatalf("Resume(): expected err to be nil, got:%s", err.Error())
	}

	waitForStatus(ex, execer.COMPLETE)

	go func(ex *PausingExecer) {
		ex.Exec(execer.Command{})
	}(ex)

	// wait till it starts running
	waitForStatus(ex, execer.RUNNING)

	pStatus := ex.Abort()
	if pStatus.State != execer.UNKNOWN {
		t.Fatalf("p.ABORT(): expected status to be UNKNOWN, got:%s", pStatus.State.String())
	}
	if pStatus.Error != "" {
		t.Fatalf("p.ABORT(): expected Error to be nil, got:%s", pStatus.Error)
	}
}

func lockingGetState(ex *PausingExecer) execer.ProcessState {
	PausingExecerMU.Lock()
	defer PausingExecerMU.Unlock()
	state := ex.State
	return state
}

// wait up to 30 Ms for the execer to reach the target state
func waitForStatus(ex *PausingExecer, targetState execer.ProcessState) {
	// check for complete every 10 milliseconds up to 3 times
	ticker := time.NewTicker(time.Millisecond * 10)
	tries := 0
	for true{
		<- ticker.C
		if lockingGetState(ex) == targetState || tries >= 3 {
			ticker.Stop()
			break
		}
		tries += 1
	}
}


