package local

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

const errorMsgFromRunner = "Error in fakeRunner Run()"

// Send a run request that pauses, then another run request.
// Verify that the status of the first run request is running, and
// the status of the second request is queued.
// Then send a signal to allow the first (paused) run request to finish.
// Wait 1ms then verify that the second run request was run.
func TestQueueing2Messages(t *testing.T) {
	env := setup(10, t)
	defer env.teardown()
	qr := env.qr
	env.wg.Add(1) // set the pause condition

	// send the first command - it should pause
	run1 := assertRun(t, qr, running(), "pause", "complete 0")
	run2 := assertRun(t, qr, pending(), "complete 1")
	env.wg.Done() // send signal to end first (paused) request
	assertWait(t, qr, run1, complete(0), "n/a")
	assertWait(t, qr, run2, complete(1), "n/a")
}

func TestQueueingMoreThanMaxMessage(t *testing.T) {
	env := setup(4, t)
	defer env.teardown()
	qr := env.qr
	env.wg.Add(1) // set the pause condition

	var runIDs []runner.RunId

	// block the runner
	runID := assertRun(t, qr, running(), "pause", "complete 0")
	runIDs = append(runIDs, runID)

	// fill the queue
	for i := 0; i < 4; i++ {
		runID := assertRun(t, qr, pending(), "complete 0")
		runIDs = append(runIDs, runID)
	}

	// overflow
	_, err := qr.Run(&runner.Command{Argv: []string{"complete 5"}})
	if err == nil || strings.Compare(QueueFullMsg, err.Error()) != 0 {
		t.Fatal("Should not be able to schedule: ", err)
	}

	// resume
	env.wg.Done()

	// drain
	for _, id := range runIDs {
		assertWait(t, qr, id, complete(0), "n/a")
	}
	runIDs = nil

	// repeat
	env.wg.Add(1)
	runID = assertRun(t, qr, running(), "pause", "complete 0")
	runIDs = append(runIDs, runID)
	for i := 0; i < 4; i++ {
		runID := assertRun(t, qr, pending(), "complete 0")
		runIDs = append(runIDs, runID)
	}
	_, err = qr.Run(&runner.Command{Argv: []string{"complete 5"}})
	if err == nil || strings.Compare(QueueFullMsg, err.Error()) != 0 {
		t.Fatal("Should not be able to schedule: ", err)
	}
	env.wg.Done()
	for _, id := range runIDs {
		assertWait(t, qr, id, complete(0), "n/a")
	}
}

func TestUnknownRunIdInStatusRequest(t *testing.T) {
	env := setup(4, t)
	defer env.teardown()
	qr := env.qr

	st, err := qr.Status(runner.RunId("not a real run id"))
	if err == nil {
		t.Fatal("Should not be able to get status", err, st)
	}
}

func TestRunnerReturningAnErrorOnRunRequest(t *testing.T) {
	env := setup(4, t)
	defer env.teardown()
	qr := env.qr
	env.wg.Add(1)

	// fill the queue
	_ = assertRun(t, qr, running(), "pause", "complete 0")
	run2 := assertRun(t, qr, pending(), "complete 0")
	run3 := assertRun(t, qr, pending(), "complete 0")
	run4 := assertRun(t, qr, pending(), "complete 0")
	run5 := assertRun(t, qr, pending(), "complete 0")

	// Now kill the connection to the runner
	env.chaos.SetError(fmt.Errorf("can't even"))
	env.wg.Done()
	// The next line will fail because we can't talk to the underlying
	// Runner anymore
	// assertWait(t, qr, run1, complete(0), "n/a")
	assertWait(t, qr, run2, failed("can't even"), "n/a")
	assertWait(t, qr, run3, failed("can't even"), "n/a")
	assertWait(t, qr, run4, failed("can't even"), "n/a")
	assertWait(t, qr, run5, failed("can't even"), "n/a")
}

func TestStatus(t *testing.T) {
	env := setup(4, t)
	defer env.teardown()
	qr := env.qr
	env.wg.Add(1)

	// We want to get lots of runs with statuses in different places:
	// *) in the queue
	// *) in errored (because delegate refuses it)
	// *) in errored (because it was aborted while in the queue)
	// *) in delegate (running, aborted, and complete)

	env.chaos.SetError(fmt.Errorf("can't even"))
	st, _ := qr.Run(&runner.Command{Argv: []string{"complete 5"}})
	run1 := st.RunId
	env.chaos.SetError(nil)
	// run1 is in errored (because delegate refused it)
	assertWait(t, qr, run1, failed("can't even"))

	run2 := assertRun(t, qr, running(), "pause", "complete 1")
	run3 := assertRun(t, qr, pending(), "complete 0")
	run4 := assertRun(t, qr, pending(), "complete 0")
	run5 := assertRun(t, qr, pending(), "complete 0")

	// run5 is in errored (because it was aborted while in the queue)
	if _, err := qr.Abort(run5); err != nil {
		t.Fatal(err)
	}
	assertWait(t, qr, run5, aborted())

	if _, err := qr.Abort(run2); err != nil {
		t.Fatal(err)
	}
	// run2 is delegated (aborted)
	assertWait(t, qr, run2, aborted())

	env.wg.Done()

	// run3 and 4 are in delegate, complete
	assertWait(t, qr, run3, complete(0))
	assertWait(t, qr, run4, complete(0))

	env.wg.Add(1)

	// run6 is in delegate, running
	assertRun(t, qr, running(), "pause", "complete 2")

	// run7 and 8 are in queue
	assertRun(t, qr, pending(), "complete 0")
	assertRun(t, qr, pending(), "complete 0")

	all, err := qr.StatusAll()
	if err != nil {
		t.Fatal(err)
	}

	// How do we check the result of StatusAll?
	// We've already waited for each status to be correct, so we trust that.
	// So, for each status in StatusAll, we'll call Status on its ID and error if not equal
	for _, st := range all {
		st2, err := qr.Status(st.RunId)
		if err != nil {
			t.Fatal(err)
		}
		if st != st2 {
			t.Fatalf("status for id %v unequal: %v vs %v", st.RunId, st, st2)
		}
	}
}

func setup(size int, t *testing.T) *env {
	runnerAvailableCh := make(chan struct{}, 1)

	wg := &sync.WaitGroup{}
	ex := execers.NewSimExecer(wg)
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Test setup() failed getting temp dir:%s", err.Error())
	}

	outputCreator, err := NewOutputCreator(tempDir)
	if err != nil {
		t.Fatalf("Test setup() failed getting output creator:%s", err.Error())
	}
	sr := NewSimpleReportBackRunner(ex, snapshots.MakeInvalidCheckouter(), outputCreator, runnerAvailableCh)
	chaos := runners.NewChaosRunner(sr)
	qr := NewQueuingRunner(chaos, size, runnerAvailableCh).(*QueueingRunner)

	return &env{chaos: chaos, wg: wg, qr: qr, sr: sr}
}

type env struct {
	chaos *runners.ChaosRunner
	wg    *sync.WaitGroup
	qr    *QueueingRunner
	sr    *simpleRunner
}

func (t *env) teardown() {
	t.sr.Close()
	t.qr.Close()
}
