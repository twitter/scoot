package os

import (
	"bytes"
	"testing"
	"time"

	"github.com/scootdev/scoot/runner/execer"
)

func TestAll(t *testing.T) {
	exer := NewExecer()

	// TODO(dbentley): factor out an assertRun method
	cmd := execer.Command{Argv: []string{"true"}}
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
		t.Fatalf("Incorrect output, got %q and %q; expected %q and \"\"", stdoutText, stderrText, stdoutExpected)
	}
}

func TestMemUsage(t *testing.T) {
	// Command to increase memory by 10MB every .1s until we hit 50MB after .5s.
	// Creates a bash process and under that a python process. They should both contribute to MemUsage.
	str := "python -c \"import time\nx=[]\nfor i in range(5):\n x.append(' ' * 10*1024*1024)\n time.sleep(.1)\" &"
	cmd := execer.Command{Argv: []string{"bash", "-c", str}}
	process, err := NewExecer().Exec(cmd)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Check for growing memory usage at [.2, .4]s. Then check that the usage is a reasonable minimum value (25MB).
	prevUsage := 0
	for i := 0; i < 2; i++ {
		time.Sleep(200 * time.Millisecond)
		if newUsage, err := memUsage(process.(*osProcess).cmd.Process.Pid); err != nil {
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

	process.Abort()
}
