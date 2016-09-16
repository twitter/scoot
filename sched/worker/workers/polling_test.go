package workers

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/snapshots/fake"
)

func TestPollingWorker_Simple(t *testing.T) {
	ex := execers.NewSimExecer(nil)
	r := local.NewSimpleRunner(ex, fake.MakeInvalidCheckouter(), runners.NewNullOutputCreator())
	w := NewPollingWorker(r, time.Duration(10)*time.Microsecond)
	st, err := w.RunAndWait(task("complete 42"))
	if err != nil || st.State != runner.COMPLETE || st.ExitCode != 42 {
		t.Fatalf("got status %v, error %v; expected {0 COMPLETE file:///dev/null file:///dev/null 42 } <nil>", st, err)
	}
}

// Test it doesn't return until the task is done
func TestPollingWorker_Wait(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	ex := execers.NewSimExecer(&wg)
	r := local.NewSimpleRunner(ex, fake.MakeInvalidCheckouter(), runners.NewNullOutputCreator())
	w := NewPollingWorker(r, time.Duration(10)*time.Microsecond)
	stCh, errCh := make(chan runner.ProcessStatus, 1), make(chan error, 1)
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

	wg.Done()
	st, err := <-stCh, <-errCh

	if err != nil || st.State != runner.COMPLETE || st.ExitCode != 43 {
		t.Fatalf("got status %v, error %v; expected {0 COMPLETE file:///dev/null file:///dev/null 43 } <nil>", st, err)
	}
}

func TestPollingWorker_ErrorRunning(t *testing.T) {
	ex := execers.NewSimExecer(nil)
	r := local.NewSimpleRunner(ex, fake.MakeInvalidCheckouter(), runners.NewNullOutputCreator())
	chaos := runners.NewChaosRunner(r)
	w := NewPollingWorker(chaos, time.Duration(10)*time.Microsecond)

	chaos.Err = fmt.Errorf("could not run")
	// Now make the runner error
	st, err := w.RunAndWait(task("complete 43"))

	if err == nil {
		t.Fatalf("got status %v, error %v; expected { UNKNOWN  0 } error could not run", st, err)
	}
}

func TestPollingWorker_ErrorPolling(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	ex := execers.NewSimExecer(&wg)
	r := local.NewSimpleRunner(ex, fake.MakeInvalidCheckouter(), runners.NewNullOutputCreator())
	chaos := runners.NewChaosRunner(r)
	w := NewPollingWorker(chaos, time.Duration(10)*time.Microsecond)
	stCh, errCh := make(chan runner.ProcessStatus, 1), make(chan error, 1)
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
	chaos.Err = fmt.Errorf("connection error")

	st, err := <-stCh, <-errCh

	if err == nil {
		t.Fatalf("got status %v, error %v; expected { UNKNOWN  0 } error connection error", st, err)
	}

	// Now let it finish
	wg.Done()
}

func task(argv ...string) sched.TaskDefinition {
	return sched.TaskDefinition{
		Command: runner.Command{
			Argv: argv,
		},
	}
}
