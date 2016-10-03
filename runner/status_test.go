package runner_test

import (
	"testing"
	"time"

	"github.com/scootdev/scoot/runner"
)

func TestCommandStringSimple(t *testing.T) {
	c := runner.Command{
		Argv:       []string{"./pants", "test", "science/src/go/twitter.biz::"},
		EnvVars:    map[string]string{"GOOS": "linux"},
		Timeout:    10 * time.Minute,
		SnapshotId: "git-abcd1234",
	}

	expected := `Command
	Snapshot ID:	git-abcd1234
	Argv:	["./pants" "test" "science/src/go/twitter.biz::"]
	Timeout:	10m0s
	Env:
		GOOS: linux
`
	if s := c.String(); s != expected {
		t.Errorf("Got:\n%s\nExpected:\n%s\n", s, expected)
	}
}

func TestProcStatusStringCompleted(t *testing.T) {
	ps := runner.ProcessStatus{
		RunId:     runner.RunId("12"),
		State:     runner.COMPLETE,
		StdoutRef: "stdout",
		StderrRef: "stderr",
		ExitCode:  9,
	}

	expected := `--- Process Status ---
	ID:		12
	State:		COMPLETE
	ExitCode:	9
	Stdout:		stdout
	Stderr:		stderr
`

	if s := ps.String(); s != expected {
		t.Errorf("Got:\n%s\nExpected:\n%s\n", s, expected)
	}
}

func TestProcStatusStringError(t *testing.T) {
	ps := runner.ProcessStatus{
		RunId:     runner.RunId("aaaaa"),
		State:     runner.FAILED,
		StdoutRef: "stdout",
		StderrRef: "stderr",
		Error:     "The thing blew up.",
	}

	expected := `--- Process Status ---
	ID:		aaaaa
	State:		FAILED
	Error:		The thing blew up.
	Stdout:		stdout
	Stderr:		stderr
`

	if s := ps.String(); s != expected {
		t.Errorf("Got:\n%s\nExpected:\n%s\n", s, expected)
	}
}
