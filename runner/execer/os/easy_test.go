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
	// Command to increase memory by 1MB every .1s until we hit 50MB after 5s.
	// Creates a bash process and under that a python process. They should both contribute to MemUsage.
	str := `import time; exec("x=[]\nfor i in range(50):\n x.append(' ' * 1024*1024)\n time.sleep(.1)")`
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
	// Check for growing memory usage at [1.5, 3]s. Then check that the usage is between a reasonable min/max.
	prevUsage := 0
	sleepDuration := 1500 * time.Millisecond
	for i := 0; i < 2; i++ {
		time.Sleep(sleepDuration)
		if newUsage, err := e.memUsage(process.(*osProcess).cmd.Process.Pid); err != nil {
			t.Fatalf(err.Error())
		} else if int(newUsage) <= prevUsage {
			t.Fatalf("Expected growing memory, got: %d -> %d @%dms", prevUsage, newUsage, (i+1)*int(sleepDuration/time.Millisecond))
		} else {
			prevUsage = int(newUsage)
		}
	}
	if prevUsage < 15*1024*1024 {
		t.Fatalf("Expected usage to be at least 20MB, was: %dB", prevUsage)
	}
	if prevUsage > 50*1024*1024 {
		t.Fatalf("Expected usage to be less than 50MB, was: %dB", prevUsage)
	}
}

func TestMemCap(t *testing.T) {
	// Command to increase memory by 1MB every .1s up to 5s.
	// Creates a bash process and under that a python process. They should both contribute to MemUsage.
	str := `import time; exec("x=[]\nfor i in range(50):\n x.append(' ' * 1024*1024)\n time.sleep(.1)")`
	memCh := make(chan execer.ProcessStatus)
	cmd := execer.Command{
		Argv: []string{"python", "-c", str},
		LogTags: tags.LogTags{
			Tag:    "tag",
			JobID:  "jobID1234",
			TaskID: "taskID1234",
		},
		MemCh: memCh,
	}
	// Terminate nearly immediately, after memory grows to 1MB.
	e := NewBoundedExecer(execer.Memory(1024*1024), stats.NilStatsReceiver())
	process, err := e.Exec(cmd)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer process.Abort()
	pid := process.(*osProcess).cmd.Process.Pid
	var usage execer.Memory
	var timeoutCh <-chan time.Time
	timeout := time.NewTimer(time.Second * 2)
	timeoutCh = timeout.C
	defer timeout.Stop()
	select {
	case <-memCh:
		if usage, err = e.memUsage(pid); err != nil {
			t.Fatalf(err.Error())
		}
		if usage != 0 {
			t.Fatalf("Expected usage to be 0MB, was: %dB", usage)
		}
	case <-timeoutCh:
		t.Fatalf("Memory usage didn't exceed memCap within 2 seconds")
	}
}
