package local_test

import (
	"fmt"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/fake"
	"github.com/scootdev/scoot/runner/local"
	// "log"
	"sync"
	"testing"
)

func TestRun(t *testing.T) {
	exec := fake.NewSimExecer(nil)
	r := local.NewSimpleRunner(exec)
	firstId := assertRun(t, r, complete(0), "complete 0")
	assertRun(t, r, complete(1), "complete 1")
	// Now make sure that the first results are still available
	assertWait(t, r, firstId, complete(0), "complete 0")
}

func TestSimul(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	ex := fake.NewSimExecer(&wg)
	r := local.NewSimpleRunner(ex)
	firstArgs := []string{"pause", "complete 0"}
	firstRun := run(t, r, firstArgs)
	st, err := r.Status(firstRun)
	if err != nil {
		t.Fatalf("can't get status for %v: %v", firstArgs, err)
	}
	assertStatus(t, st, running(), firstArgs...)

	// Now that one is running, try running a second
	secondArgs := []string{"complete 3"}
	cmd := &runner.Command{}
	cmd.Argv = secondArgs
	st, err = r.Run(cmd)
	if err != nil {
		t.Fatalf("Couldn't run %v: %v", secondArgs, err)
	}
	assertStatus(t, st, failed("Runner is busy"), secondArgs...)

	wg.Done()
	assertWait(t, r, firstRun, complete(0), firstArgs...)
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

func assertRun(t *testing.T, r runner.Runner, expected runner.ProcessStatus, args ...string) runner.RunId {
	runId := run(t, r, args)
	assertWait(t, r, runId, expected, args...)
	return runId
}

func assertWait(t *testing.T, r runner.Runner, runId runner.RunId, expected runner.ProcessStatus, args ...string) {
	actual, err := wait(r, runId)
	if err != nil {
		t.Fatalf("error waiting (cmd:%v)", args)
	}
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
}

func run(t *testing.T, r runner.Runner, args []string) runner.RunId {
	cmd := &runner.Command{}
	cmd.Argv = args
	status, err := r.Run(cmd)
	if err != nil {
		t.Fatal("Couldn't run: ", args, err)
	}
	return status.RunId
}

func wait(r runner.Runner, run runner.RunId) (runner.ProcessStatus, error) {
	for {
		status, err := r.Status(run)
		if err != nil || status.State.IsDone() {
			return status, err
		}
	}
}
