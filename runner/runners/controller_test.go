package runners

import (
	"fmt"
	"testing"

	"github.com/scootdev/scoot/runner"
)

// Utilities for testing controllers (single_test.go and queue_test.go)

func complete(exitCode int) runner.ProcessStatus {
	return runner.CompleteStatus(runner.RunId(""), "", exitCode)
}

func pending() runner.ProcessStatus {
	return runner.PendingStatus(runner.RunId(""))
}

func running() runner.ProcessStatus {
	return runner.RunningStatus(runner.RunId(""), "", "")
}

func failed(errorText string) runner.ProcessStatus {
	return runner.ErrorStatus(runner.RunId(""), fmt.Errorf(errorText))
}

func aborted() runner.ProcessStatus {
	return runner.AbortStatus(runner.RunId(""))
}

func badRequest(errorText string) runner.ProcessStatus {
	return runner.BadRequestStatus(runner.RunId(""), fmt.Errorf(errorText))
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
	status, err := r.Run(cmd)
	if err != nil {
		t.Fatal("Couldn't run: ", args, err)
	}
	return status.RunId
}

func wait(r runner.Runner, run runner.RunId, expected runner.ProcessStatus) runner.ProcessStatus {
	st, err := r.StatusQuerySingle(runner.RunState(run, expected.State), runner.Wait())
	if err != nil {
		panic(err)
	}
	return st
}
