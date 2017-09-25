package runners

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

// Send a run request that pauses, then another run request.
// Verify that the status of the first run request is running, and
// the status of the second request is queued.
// Then send a signal to allow the first (paused) run request to finish.
// Wait that all are run
func TestQueueing2Messages(t *testing.T) {
	env := setup(10, snapshot.NoDuration, t)
	defer env.teardown()

	// send the first command - it should pause
	run1 := assertRun(t, env.r, running(), "pause", "complete 0")
	run2 := assertRun(t, env.r, pending(), "complete 1")
	env.sim.Resume()
	assertWait(t, env.r, run1, complete(0), "n/a")
	assertWait(t, env.r, run2, complete(1), "n/a")
}

func TestQueueingMoreThanMaxMessage(t *testing.T) {
	env := setup(4, snapshot.NoDuration, t)
	defer env.teardown()

	var runIDs []runner.RunID

	// block the runner
	runID := assertRun(t, env.r, running(), "pause", "complete 0")
	runIDs = append(runIDs, runID)

	// fill the queue
	for i := 0; i < 3; i++ {
		runID := assertRun(t, env.r, pending(), "complete 0")
		runIDs = append(runIDs, runID)
	}

	// overflow
	_, err := env.r.Run(&runner.Command{Argv: []string{"complete 5"}})
	if err == nil || strings.Compare(QueueFullMsg, err.Error()) != 0 {
		t.Fatal("Should not be able to schedule: ", err)
	}

	// resume
	env.sim.Resume()

	// drain
	for _, id := range runIDs {
		assertWait(t, env.r, id, complete(0), "n/a")
	}

	// repeat
	runID = assertRun(t, env.r, running(), "pause", "complete 0")
	runIDs = append(runIDs, runID)
	for i := 0; i < 3; i++ {
		runID := assertRun(t, env.r, pending(), "complete 0")
		runIDs = append(runIDs, runID)
	}
	_, err = env.r.Run(&runner.Command{Argv: []string{"complete 5"}})
	if err == nil || strings.Compare(QueueFullMsg, err.Error()) != 0 {
		t.Fatal("Should not be able to schedule: ", err)
	}
	env.sim.Resume()
	for _, id := range runIDs {
		assertWait(t, env.r, id, complete(0), "n/a")
	}
}

func TestUnknownRunIDInStatusRequest(t *testing.T) {
	env := setup(4, snapshot.NoDuration, t)
	defer env.teardown()

	st, _, err := env.r.Status(runner.RunID("not a real run id"))
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf(UnknownRunIDMsg, "")) {
		t.Fatalf("Should not be able to get status: %q %q", err, st)
	}
}

func TestStatus(t *testing.T) {
	env := setup(4, snapshot.NoDuration, t)
	defer env.teardown()

	// We want to get lots of runs with statuses in different places:
	// *) in the queue
	// *) in errored (because delegate refuses it)
	// *) in errored (because it was aborted while in the queue)
	// *) in delegate (running, aborted, and complete)

	assertRun(t, env.r, complete(5), "complete 5")

	run2 := assertRun(t, env.r, running(), "pause", "complete 1")
	run3 := assertRun(t, env.r, pending(), "complete 0")
	run4 := assertRun(t, env.r, pending(), "complete 0")
	run5 := assertRun(t, env.r, pending(), "complete 0")

	// run5 is in errored (because it was aborted while in the queue)
	if _, err := env.r.Abort(run5); err != nil {
		t.Fatal(err)
	}
	assertWait(t, env.r, run5, aborted())

	if _, err := env.r.Abort(run2); err != nil {
		t.Fatal(err)
	}
	// run2 is delegated (aborted)
	assertWait(t, env.r, run2, aborted())

	// run3 and 4 are in delegate, complete
	assertWait(t, env.r, run3, complete(0))
	assertWait(t, env.r, run4, complete(0))

	// run6 is in delegate, running
	run6 := assertRun(t, env.r, running(), "pause", "complete 2")

	// run7 and 8 are in queue
	run7 := assertRun(t, env.r, pending(), "complete 0")
	run8 := assertRun(t, env.r, pending(), "complete 0")

	all, _, err := env.r.StatusAll()
	if err != nil {
		t.Fatal(err)
	}

	// How do we check the result of StatusAll?
	// We've already waited for each status to be correct, so we trust that.
	// So, for each status in StatusAll, we'll call Status on its ID and error if not equal
	for _, st := range all {
		st2, _, err := env.r.Status(st.RunID)
		if err != nil {
			t.Fatal(err)
		}
		if st != st2 {
			t.Fatalf("status for id %v unequal: %v vs %v", st.RunID, st, st2)
		}
	}

	env.sim.Resume()
	assertWait(t, env.r, run6, complete(2))
	assertWait(t, env.r, run7, complete(0))
	assertWait(t, env.r, run8, complete(0))
}

func TestUpdaterNoUpdate(t *testing.T) {
	env := setup(1, snapshot.NoDuration, t)
	defer env.teardown()

	run1 := assertRun(t, env.r, running(), "pause", "complete 0")
	env.sim.Resume()
	assertWait(t, env.r, run1, complete(0), "n/a")

	if *env.uc != 0 {
		t.Fatalf("Unexpected Updates occurred (%d times)", *env.uc)
	}
}

// Note: it's not currently possible to test precise occurrences of Updates and
// Task runs, because ultimately the appearance of Updates() is dependent on a time.Ticker,
// which will invariably fail in go's race detector, but this is not a problem in practice
func TestUpdaterPeriodic(t *testing.T) {
	env := setup(1, 10*time.Millisecond, t)
	defer env.teardown()

	env.u.WaitForUpdateRunning()
	env.u.Unpause()
	env.u.WaitForUpdateRunning()
	env.u.Unpause()
	env.u.WaitForUpdateRunning()

	if *env.uc != 2 {
		t.Fatalf("Expected to get 2 updates, got: %d", *env.uc)
	}
}

func TestAbortQueuedCommand(t *testing.T) {
	env := setup(2, 10*time.Millisecond, t)
	defer env.teardown()

	// block the queue with an update
	env.u.WaitForUpdateRunning()
	run1 := assertRun(t, env.r, pending(), "complete 0")
	assertRun(t, env.r, pending(), "complete 0")

	if _, err := env.r.Abort(run1); err != nil {
		t.Fatal(err)
	}

	env.u.Unpause()
	assertWait(t, env.r, run1, aborted())
}

func setup(capacity int, interval time.Duration, t *testing.T) *env {
	sim := execers.NewSimExecer()
	tmpDir, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Test setup() failed getting temp dir:%s", err.Error())
	}

	outputCreator, err := NewHttpOutputCreator(tmpDir, "")
	if err != nil {
		t.Fatalf("Test setup() failed getting output creator:%s", err.Error())
	}

	updateCount := 0
	updater := snapshots.MakeCountingUpdater(&updateCount, interval, true).(*snapshots.CountingUpdater)
	r := NewQueueRunner(sim, snapshots.MakeInvalidFilerUpdater(updater), nil, outputCreator, tmpDir, capacity, nil)

	return &env{sim: sim, r: r, u: updater, uc: &updateCount}
}

type env struct {
	sim *execers.SimExecer
	r   runner.Service
	u   *snapshots.CountingUpdater
	uc  *int
}

func (t *env) teardown() {
}
