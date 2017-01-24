package workers

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

var tmp *temp.TempDir

func TestMain(m *testing.M) {
	tmp, _ = temp.NewTempDir("", "worker_adapter_test")
	os.Exit(m.Run())
}

func TestPollingWorker_Simple(t *testing.T) {
	ex := execers.NewSimExecer()
	r := runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), runners.NewNullOutputCreator(), tmp)
	w := NewServiceWorker(r, time.Second, time.Duration(10)*time.Microsecond)
	st, err := w.RunAndWait(task("complete 42"))
	if err != nil || st.State != runner.COMPLETE || st.ExitCode != 42 {
		t.Fatalf("got status %v, error %v; expected {0 COMPLETE file:///dev/null file:///dev/null 42 } <nil>", st, err)
	}
}

// Test it doesn't return until the task is done
func TestPollingWorker_Wait(t *testing.T) {
	ex := execers.NewSimExecer()
	r := runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), runners.NewNullOutputCreator(), tmp)
	w := NewServiceWorker(r, time.Second, time.Duration(10)*time.Microsecond)
	stCh, errCh := make(chan runner.RunStatus, 1), make(chan error, 1)
	go func() {
		st, err := w.RunAndWait(task("pause", "complete 43"))
		stCh <- st
		errCh <- err
	}()

	// Sleep for long enough to poll a few times
	time.Sleep(time.Duration(10) * time.Millisecond)
	select {
	case st := <-stCh:
		err := <-errCh
		t.Fatal("should still be waiting", st, err)
	default:
	}

	ex.Resume()
	st, err := <-stCh, <-errCh

	if err != nil || st.State != runner.COMPLETE || st.ExitCode != 43 {
		t.Fatalf("got status %v, error %v; expected {0 COMPLETE file:///dev/null file:///dev/null 43 } <nil>", st, err)
	}
}

func TestPollingWorker_ErrorRunning(t *testing.T) {
	ex := execers.NewSimExecer()
	r := runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), runners.NewNullOutputCreator(), tmp)
	chaos := runners.NewChaosRunner(r)
	w := NewServiceWorker(chaos, 0, time.Duration(10)*time.Microsecond)

	chaos.SetError(fmt.Errorf("could not run"))
	// Now make the runner error
	st, err := w.RunAndWait(task("complete 43"))

	if err == nil {
		t.Fatalf("got status %v, error %v; expected { UNKNOWN  0 } error could not run", st, err)
	}
}

func TestPollingWorker_ErrorPolling(t *testing.T) {
	ex := execers.NewSimExecer()
	r := runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), runners.NewNullOutputCreator(), tmp)
	chaos := runners.NewChaosRunner(r)
	poller := runners.NewPollingService(chaos, chaos, chaos, 2*time.Microsecond)
	w := NewServiceWorker(poller, time.Second, time.Duration(10)*time.Microsecond)
	stCh, errCh := make(chan runner.RunStatus, 1), make(chan error, 1)
	go func() {
		st, err := w.RunAndWait(task("pause", "complete 43"))
		stCh <- st
		errCh <- err
	}()

	// Sleep for long enough to poll a few times
	time.Sleep(time.Duration(10) * time.Millisecond)
	select {
	case st := <-stCh:
		err := <-errCh
		t.Fatal("should still be waiting", st, err)
	default:
	}

	// Now make the runner error
	chaos.SetError(fmt.Errorf("connection error"))

	st, err := <-stCh, <-errCh

	if err == nil {
		t.Fatalf("got status %v, error %v; expected { UNKNOWN  0 } error connection error", st, err)
	}

	// Now let it finish
	ex.Resume()
}

func TestPollingWorker_Timeout(t *testing.T) {
	ex := execers.NewSimExecer()
	r := runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), runners.NewNullOutputCreator(), tmp)

	w := NewServiceWorker(r, time.Duration(20)*time.Microsecond, time.Duration(2)*time.Microsecond)

	task := sched.TaskDefinition{
		Command: runner.Command{
			Argv:    []string{"sleep 500", "complete 43"},
			Timeout: 50 * time.Microsecond,
		},
	}

	status, err := w.RunAndWait(task)
	if err != nil {
		t.Errorf("Received Unexpected Error: %v", err)
	}

	if status.State != runner.TIMEDOUT {
		t.Errorf("Expected status state to be TIMEDOUT. Status: %+v", status)
	}
}

func task(argv ...string) sched.TaskDefinition {
	return sched.TaskDefinition{
		Command: runner.Command{
			Argv: argv,
		},
	}
}
