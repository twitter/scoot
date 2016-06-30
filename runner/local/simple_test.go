package local_test

import (
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/fake"
	"github.com/scootdev/scoot/runner/local"
	"log"
	"testing"
)

func TestRun(t *testing.T) {
	exec := fake.NewSimExecer()
	r := local.NewSimpleRunner(exec)
	firstId := assertRun(t, r, complete(0), "complete 0")
	assertRun(t, r, complete(1), "complete 1")
	// Now make sure that the first results are still available
	assertWait(t, r, firstId, complete(0), "complete 0")

}

func complete(exitCode int) runner.ProcessStatus {
	return runner.CompleteStatus(runner.RunId(""), "", "", exitCode)
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
	if expected.State != actual.State {
		t.Fatalf("expected stated %v; was: %v (cmd:%v)", expected.State, actual.State, args)
	}
	if expected.State == runner.COMPLETE && expected.ExitCode != actual.ExitCode {
		t.Fatalf("expected exit code %v; was: %v (cmd:%v)", expected.ExitCode, actual.ExitCode, args)
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
		log.Println("status", run, err, status.State, status.State.IsDone())
		if err != nil || status.State.IsDone() {
			return status, err
		}
	}
}
