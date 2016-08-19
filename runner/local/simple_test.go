package local_test

import (
	"fmt"
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/fake"
	"github.com/scootdev/scoot/runner/local"
	fakesnaps "github.com/scootdev/scoot/snapshots/fake"

	"sync"
	"testing"
)

func TestRun(t *testing.T) {
	exec := fake.NewSimExecer(nil)
	r := local.NewSimpleRunner(exec, fakesnaps.MakeInvalidCheckouter())
	firstId := assertRun(t, r, complete(0), "complete 0")
	assertRun(t, r, complete(1), "complete 1")
	// Now make sure that the first results are still available
	assertWait(t, r, firstId, complete(0), "complete 0")
}

func TestSimul(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	ex := fake.NewSimExecer(&wg)
	r := local.NewSimpleRunner(ex, fakesnaps.MakeInvalidCheckouter())
	firstArgs := []string{"pause", "complete 0"}
	firstRun := run(t, r, firstArgs)
	assertWait(t, r, firstRun, running(), firstArgs...)

	// Now that one is running, try running a second
	secondArgs := []string{"complete 3"}
	cmd := &runner.Command{}
	cmd.Argv = secondArgs
	st := r.Run(cmd)
	assertStatus(t, st, badreq("Runner is busy"), secondArgs...)

	wg.Done()
	assertWait(t, r, firstRun, complete(0), firstArgs...)
}

func TestAbort(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	ex := fake.NewSimExecer(&wg)
	r := local.NewSimpleRunner(ex, fakesnaps.MakeInvalidCheckouter())
	args := []string{"pause", "complete 0"}
	runId := run(t, r, args)
	assertWait(t, r, runId, running(), args...)
	r.Abort(runId)
	// use r.Status isntead of assertWait so that we make sure it's aborted immediately, not eventually
	st := r.Status(runId)
	assertStatus(t, st, aborted(), args...)

	st = r.Abort(runner.RunId("not-a-run-id"))
	assertStatus(t, st, badreq("cannot find run not-a-run-id"))
}

func complete(exitCode int) runner.ProcessStatus {
	return runner.CompleteStatus(runner.RunId(""), "", "", exitCode)
}

func running() runner.ProcessStatus {
	return runner.RunningStatus(runner.RunId(""))
}

func failed(errorText string) runner.ProcessStatus {
	return runner.ErrorStatus(runner.RunId(""), fmt.Errorf(errorText))
}

func badreq(errorText string) runner.ProcessStatus {
	return runner.BadRequestStatus(runner.RunId(""), fmt.Errorf(errorText))
}

func aborted() runner.ProcessStatus {
	return runner.AbortStatus(runner.RunId(""))
}

func assertRun(t *testing.T, r runner.Runner, expected runner.ProcessStatus, args ...string) runner.RunId {
	runId := run(t, r, args)
	assertWait(t, r, runId, expected, args...)
	return runId
}

func assertWait(t *testing.T, r runner.Runner, runId runner.RunId, expected runner.ProcessStatus, args ...string) {
	actual := wait(r, runId, expected)
	assertStatus(t, actual, expected, args...)
}

func assertStatus(t *testing.T, actual runner.ProcessStatus, expected runner.ProcessStatus, args ...string) {
	if expected.State != actual.State {
		t.Fatalf("expected state %v; was: %v (cmd:%v)", expected.State, actual.State, args)
	}
	if expected.State == runner.COMPLETE && expected.ExitCode != actual.ExitCode {
		t.Fatalf("expected exit code %v; was: %v (cmd:%v)", expected.ExitCode, actual.ExitCode, args)
	}
	if expected.State == runner.FAILED && expected.Error != actual.Error {
		t.Fatalf("expected error %v; was: %v (cmd:%v)", expected.Error, actual.Error, args)
	}
	if expected.State == runner.BADREQUEST && expected.Error != actual.Error {
		t.Fatalf("expected error %v; was: %v (cmd:%v)", expected.Error, actual.Error, args)
	}
}

func run(t *testing.T, r runner.Runner, args []string) runner.RunId {
	cmd := &runner.Command{}
	cmd.Argv = args
	status := r.Run(cmd)
	if status.State == runner.BADREQUEST {
		t.Fatal("Couldn't run: ", args, status.Error)
	}
	return status.RunId
}

func wait(r runner.Runner, run runner.RunId, expected runner.ProcessStatus) runner.ProcessStatus {
	for {
		time.Sleep(100 * time.Microsecond)
		status := r.Status(run)
		if status.State.IsDone() || status.State == expected.State {
			return status
		}
	}
}
