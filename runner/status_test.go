package runner

import (
	"testing"
	"time"
)

func TestCommandStringSimple(t *testing.T) {
	c := Command{
		Argv:       []string{"./pants", "test", "science/src/go/twitter.biz::"},
		EnvVars:    map[string]string{"GOOS": "linux"},
		Timeout:    10 * time.Minute,
		SnapshotID: "git-abcd1234",
	}

	expected := `Command
	SnapshotID:	git-abcd1234
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
	ps := RunStatus{
		RunID:      RunID("12"),
		SnapshotID: "21",
		State:      COMPLETE,
		StdoutRef:  "stdout",
		StderrRef:  "stderr",
		ExitCode:   9,
	}

	expected := `--- Run Status ---
	Run:		12
	Snapshot:		21
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
	ps := RunStatus{
		RunID:      RunID("aaaaa"),
		SnapshotID: "bb",
		State:      FAILED,
		StdoutRef:  "stdout",
		StderrRef:  "stderr",
		Error:      "The thing blew up.",
	}

	expected := `--- Run Status ---
	Run:		aaaaa
	Snapshot:		bb
	State:		FAILED
	Error:		The thing blew up.
	Stdout:		stdout
	Stderr:		stderr
`

	if s := ps.String(); s != expected {
		t.Errorf("Got:\n%s\nExpected:\n%s\n", s, expected)
	}
}
