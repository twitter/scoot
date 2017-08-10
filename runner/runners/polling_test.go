package runners

import (
	"fmt"
	"testing"
	"time"

	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/snapshot/snapshots"
)

func setupPoller() (*execers.SimExecer, *ChaosRunner, runner.Service) {
	tmp, _ := temp.NewTempDir("", "runner_polling_test")
	ex := execers.NewSimExecer()
	single := NewSingleRunner(ex, snapshots.MakeInvalidFiler(), nil, NewNullOutputCreator(), tmp, nil)
	chaos := NewChaosRunner(single)
	var nower runner.StatusQueryNower
	nower = chaos
	poller := NewPollingService(chaos, chaos, nower, 10*time.Microsecond)
	return ex, chaos, poller
}

func TestPollingWorker_Simple(t *testing.T) {
	_, _, poller := setupPoller()

	st, err := poller.Run(&runner.Command{Argv: []string{"complete 42"}})
	if err != nil {
		t.Fatal(st, err)
	}
	st, _, err = runner.FinalStatus(poller, st.RunID)
	if err != nil {
		t.Fatal(st, err)
	}
	if st.State != runner.COMPLETE || st.ExitCode != 42 {
		t.Fatal(st, err)
	}
}

// Test it doesn't return until the task is done
func TestPollingWorker_Wait(t *testing.T) {
	ex, _, poller := setupPoller()
	stCh, errCh := make(chan runner.RunStatus, 1), make(chan error, 1)
	st, err := poller.Run(&runner.Command{Argv: []string{"pause", "complete 43"}})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		st, _, err := runner.FinalStatus(poller, st.RunID)
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
	st, err = <-stCh, <-errCh
	if err != nil || st.State != runner.COMPLETE || st.ExitCode != 43 {
		t.Fatal(st, err)
	}

}

func TestPollingWorker_Timeout(t *testing.T) {
	_, _, poller := setupPoller()
	stCh, errCh := make(chan runner.RunStatus), make(chan error)
	st, err := poller.Run(&runner.Command{Argv: []string{"sleep 1000"}, Timeout: time.Millisecond * 20})

	if err != nil {
		t.Fatal(err)
	}

	go func() {
		st, _, err := runner.FinalStatus(poller, st.RunID)
		stCh <- st
		errCh <- err
	}()

	// Sleep for long enough to poll a few times
	time.Sleep(time.Duration(10) * time.Millisecond)
	select {
	case st := <-stCh:
		err := <-errCh
		t.Log("should still be waiting:", st, err)
	default:
	}

	st, err = <-stCh, <-errCh
	if err != nil || st.State != runner.TIMEDOUT {
		t.Fatal(st, err)
	}
}

func TestPollingWorker_ErrorRunning(t *testing.T) {
	_, chaos, poller := setupPoller()
	chaos.SetError(fmt.Errorf("connection error"))

	st, err := poller.Run(&runner.Command{Argv: []string{"pause", "complete 43"}})
	if err == nil {
		t.Fatal(st, err)
	}
}

func TestPolling_ErrorPolling(t *testing.T) {
	ex, chaos, poller := setupPoller()

	stCh, errCh := make(chan runner.RunStatus, 1), make(chan error, 1)
	st, err := poller.Run(&runner.Command{Argv: []string{"pause", "complete 43"}})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		st, _, err := runner.FinalStatus(poller, st.RunID)
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

	st, err = <-stCh, <-errCh

	if err == nil {
		t.Fatalf("got status %v, error %v; expected { UNKNOWN  0 } error connection error", st, err)
	}

	// Now let it finish
	ex.Resume()

}
