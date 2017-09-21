package runner

import (
	"testing"
	"time"

	"github.com/twitter/scoot/common/log/tags"
)

func TestCommandStringSimple(t *testing.T) {
	c := Command{
		Argv:    []string{"./run", "a", "--command"},
		EnvVars: map[string]string{"GOOS": "linux"},
		Timeout: 10 * time.Minute,
		LogTags: tags.LogTags{
			JobID:  "job-abcd1234",
			TaskID: "task-abcd1234",
			Tag:    "req-abcd1234",
		},
		SnapshotID: "git-abcd1234",
	}

	expected := `runner.Command -- SnapshotID: git-abcd1234 # Argv: ["./run" "a" "--command"] # Timeout: 10m0s` +
		` # JobID: job-abcd1234 # TaskID: task-abcd1234 # Tag: req-abcd1234 # Env:  GOOS=linux`
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
	ps.JobID = "46"
	ps.TaskID = "2"
	ps.Tag = "tag"

	expected := `RunStatus -- RunID: 12 # SnapshotID: 21 # State: COMPLETE # JobID: 46 # TaskID: 2` +
		` # Tag: tag # ExitCode: 9 # Stdout: stdout # Stderr: stderr`

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
	ps.JobID = "cdefg"
	ps.TaskID = "hijkl"
	ps.Tag = "tag"

	expected := `RunStatus -- RunID: aaaaa # SnapshotID: bb # State: FAILED # JobID: cdefg # TaskID: hijkl` +
		` # Tag: tag # Error: The thing blew up. # Stdout: stdout # Stderr: stderr`

	if s := ps.String(); s != expected {
		t.Errorf("Got:\n%s\nExpected:\n%s\n", s, expected)
	}
}
