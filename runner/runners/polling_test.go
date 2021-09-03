package runners

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

func setupPoller() (*execers.SimExecer, *ChaosRunner, runner.Service) {
	ex := execers.NewSimExecer()
	filerMap := runner.MakeRunTypeMap()
	filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: snapshots.MakeInvalidFiler(), IDC: nil}
	single := NewSingleRunner(ex, filerMap, NewNullOutputCreator(), nil, stats.NopDirsMonitor, runner.EmptyID)
	chaos := NewChaosRunner(single)
	poller := NewPollingService(chaos, chaos, 10*time.Microsecond)
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

func TestPollingWorker_Intervalse(t *testing.T) {
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

func setupTestPollerIntervals(period time.Duration) (*execers.SimExecer, runner.StatusQueryNower, runner.Service) {
	ex := execers.NewSimExecer()
	filerMap := runner.MakeRunTypeMap()
	filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: snapshots.MakeInvalidFiler(), IDC: nil}
	single := NewSingleRunner(ex, filerMap, NewNullOutputCreator(), nil, stats.NopDirsMonitor, runner.EmptyID)
	chaos := NewChaosRunner(single)
	var nower runner.StatusQueryNower
	nower = &instrumentedNower{ChaosRunner: *chaos}
	poller := NewPollingService(chaos, nower, period)
	return ex, nower, poller
}

type instrumentedNower struct {
	ChaosRunner
	queryNowFreq FreqList
	lastQueryNow time.Time
}

type FreqList []time.Duration

func (f FreqList) Len() int           { return len(f) }
func (f FreqList) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f FreqList) Less(i, j int) bool { return f[i] <= f[j] }

func (in *instrumentedNower) QueryNow(q runner.Query) ([]runner.RunStatus, runner.ServiceStatus, error) {
	if in.queryNowFreq == nil {
		in.queryNowFreq = []time.Duration{}
	} else {
		in.queryNowFreq = append(in.queryNowFreq, time.Since(in.lastQueryNow))
	}
	in.lastQueryNow = time.Now()
	return in.ChaosRunner.QueryNow(q)
}

// Test poller period increases
func TestPollingWorker_QueryIntervalIncrease(t *testing.T) {
	_, nower, poller := setupTestPollerIntervals(10 * time.Microsecond)
	st, err := poller.Run(&runner.Command{Argv: []string{"complete 43"}})
	if err != nil {
		t.Fatal(err)
	}

	// Sleep for long enough to poll multiple times
	time.Sleep(time.Duration(100) * time.Microsecond)

	st, _, err = runner.FinalStatus(poller, st.RunID)
	if err != nil || st.State != runner.COMPLETE || st.ExitCode != 43 {
		t.Fatal(st, err)
	}

	inNower, ok := nower.(*instrumentedNower)
	assert.True(t, ok)
	assert.True(t, len(inNower.queryNowFreq) > 0)

	// Check for increasing duration
	assert.True(t, sort.IsSorted(inNower.queryNowFreq))
}
