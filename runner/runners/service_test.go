package runners

import (
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/scootdev/scoot/runner"
)

// Utilities for testing a runner.Service (single_test.go and queue_test.go)

func teardown(t *testing.T) {
	if p := recover(); p != nil {
		t.Fatalf("Error testing: %v\nStacktrace: %s\n", p, debug.Stack())
	}
}

func complete(exitCode int) runner.RunStatus {
	return runner.CompleteStatus(runner.RunID(""), "", exitCode)
}

func pending() runner.RunStatus {
	return runner.PendingStatus(runner.RunID(""))
}

func running() runner.RunStatus {
	return runner.RunningStatus(runner.RunID(""), "", "")
}

func failed(errorText string) runner.RunStatus {
	return runner.ErrorStatus(runner.RunID(""), fmt.Errorf(errorText))
}

func aborted() runner.RunStatus {
	return runner.AbortStatus(runner.RunID(""))
}

func badRequest(errorText string) runner.RunStatus {
	return runner.BadRequestStatus(runner.RunID(""), fmt.Errorf(errorText))
}

func assertRun(t *testing.T, r runner.Runner, expected runner.RunStatus, args ...string) runner.RunID {
	runId := run(t, r, args)
	assertWait(t, r, runId, expected, args...)
	return runId
}

func assertWait(t *testing.T, r runner.Runner, runId runner.RunID, expected runner.RunStatus, args ...string) {
	actual := wait(r, runId, expected)
	assertStatus(t, actual, expected, args...)
}

func Fatalf(f string, i ...interface{}) {
	panic(fmt.Errorf(f, i...))
}

func assertStatus(t *testing.T, actual runner.RunStatus, expected runner.RunStatus, args ...string) {
	if expected.State != actual.State {
		Fatalf("expected state %v; was: %v (cmd:%v)", expected.State, actual.State, args)
	}
	if expected.State == runner.COMPLETE && expected.ExitCode != actual.ExitCode {
		Fatalf("expected exit code %v; was: %v (cmd:%v)", expected.ExitCode, actual.ExitCode, args)
	}
	if expected.State == runner.FAILED && expected.Error != actual.Error {
		Fatalf("expected error %v; was: %v (cmd:%v)", expected.Error, actual.Error, args)
	}
	if expected.State == runner.BADREQUEST && expected.Error != actual.Error {
		Fatalf("expected error %v; was: %v (cmd:%v)", expected.Error, actual.Error, args)
	}
}

func run(t *testing.T, r runner.Runner, args []string) runner.RunID {
	cmd := &runner.Command{}
	cmd.Argv = args
	status, err := r.Run(cmd)
	if err != nil {
		Fatalf("Couldn't run: %v %v", args, err)
	}
	return status.RunID
}

func wait(r runner.Runner, run runner.RunID, expected runner.RunStatus) runner.RunStatus {
	st, err := runner.WaitForState(r, run, expected.State)
	if err != nil {
		panic(err)
	}
	return st
}
