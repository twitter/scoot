package os

import (
	"bytes"
	"testing"
	"time"

	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner/execer"
)

func TestAll(t *testing.T) {
	exer := NewExecer()

	// TODO(dbentley): factor out an assertRun method
	cmd := execer.Command{
		Argv: []string{"true"},
		LogTags: tags.LogTags{
			Tag:    "tag",
			JobID:  "jobID1234",
			TaskID: "taskID1234",
		},
	}
	p, err := exer.Exec(cmd)
	if err != nil {
		t.Fatalf("Couldn't run true %v", err)
	}
	status := p.Wait()
	if status.State != execer.COMPLETE || status.ExitCode != 0 {
		t.Fatalf("Got unexpected status running true %v", status)
	}

	cmd = execer.Command{Argv: []string{"false"}}
	p, err = exer.Exec(cmd)
	if err != nil {
		t.Fatalf("Couldn't run false %v", err)
	}
	status = p.Wait()
	if status.State != execer.COMPLETE || status.ExitCode != 1 {
		t.Fatalf("Got unexpected status running false %v", status)
	}

}

func TestOutput(t *testing.T) {
	exer := NewExecer()

	var stdout, stderr bytes.Buffer

	stdoutExpected := "hello world\n"
	// TODO(dbentley): factor out an assertRun method
	cmd := execer.Command{
		Argv:   []string{"echo", "-n", stdoutExpected},
		Stdout: &stdout,
		Stderr: &stderr,
		LogTags: tags.LogTags{
			Tag:    "tag",
			JobID:  "jobID1234",
			TaskID: "taskID1234",
		},
	}
	p, err := exer.Exec(cmd)
	if err != nil {
		t.Fatalf("Couldn't run true %v", err)
	}
	status := p.Wait()
	if status.State != execer.COMPLETE || status.ExitCode != 0 {
		t.Fatalf("Got unexpected status running true %v", status)
	}
	stdoutText, stderrText := stdout.String(), stderr.String()
	if stdoutText != stdoutExpected || stderrText != "" {
		t.Fatalf("Incorrect output, got %q and %q; expected %q and \"\"",
			stdoutText, stderrText, stdoutExpected)
	}
}

func TestMemUsage(t *testing.T) {
	// Command to increase memory by 10MB every .1s until we hit 100MB after 1s.
	// Creates a bash process and under that a python process. They should both contribute to MemUsage.
	str := `import time; exec("x=[]\nfor i in range(10):\n x.append(' ' * 10*1024*1024)\n time.sleep(.1)")`
	cmd := execer.Command{
		Argv: []string{"python", "-c", str},
		LogTags: tags.LogTags{
			Tag:    "tag",
			JobID:  "jobID1234",
			TaskID: "taskID1234",
		},
	}
	e := NewExecer()
	process, err := e.Exec(cmd)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer process.Abort()
	// Check for growing memory usage at [.2, .4]s. Then check that the usage is a reasonable minimum value (25MB)
	// and a reasonable maximum value (75MB)
	prevUsage := 0
	for i := 0; i < 2; i++ {
		time.Sleep(200 * time.Millisecond)
		if newUsage, err := e.memUsage(process.(*osProcess).cmd.Process.Pid); err != nil {
			t.Fatalf(err.Error())
		} else if int(newUsage) <= prevUsage {
			t.Fatalf("Expected growing memory, got: %d -> %d @%dms", prevUsage, newUsage, (i+1)*200)
		} else {
			prevUsage = int(newUsage)
		}
	}
	if prevUsage < 25*1024*1024 {
		t.Fatalf("Expected usage to be at least 25MB, was: %dB", prevUsage)
	}
	if prevUsage > 75*1024*1024 {
		t.Fatalf("Expected usage to be less than 75MB, was: %dB", prevUsage)
	}
}

func TestMemCap(t *testing.T) {
	// Command to increase memory by 10MB every .1s up to 5s.
	// Creates a bash process and under that a python process. They should both contribute to MemUsage.
	str := `import time; exec("x=[]\nfor i in range(50):\n x.append(' ' * 10*1024*1024)\n time.sleep(.1)")`
	cmd := execer.Command{
		Argv: []string{"python", "-c", str},
		LogTags: tags.LogTags{
			Tag:    "tag",
			JobID:  "jobID1234",
			TaskID: "taskID1234",
		},
	}
	e := NewBoundedExecer(execer.Memory(5*1024*1024), stats.NilStatsReceiver())
	process, err := e.Exec(cmd)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer process.Abort()
	pid := process.(*osProcess).cmd.Process.Pid
	// Sleep to give bounded execer time to kill process and release memory
	time.Sleep(2 * time.Second)
	var usage execer.Memory
	if usage, err = e.memUsage(pid); err != nil {
		t.Fatalf(err.Error())
	}
	if usage != 0 {
		t.Fatalf("Expected usage to be 0MB, was: %dB", usage)
	}
}
