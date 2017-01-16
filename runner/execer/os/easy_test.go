package os_test

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/scootdev/scoot/runner/execer"
	os_execer "github.com/scootdev/scoot/runner/execer/os"
)

func TestAll(t *testing.T) {
	exer := os_execer.NewExecer()

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
	exer := os_execer.NewExecer()

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
	// Command to increase memory by 1MB every .01s until we hit 25MB after .25s.
	// Creates a bash process and under that a python process. They should both contribute to MemUsage.
	str := "python -c \"import time\nx=[]\nfor i in range(25):\n x.append(' ' * 1024*1024)\n time.sleep(.01)\""
	cmd := execer.Command{Argv: []string{"bash", "-c", str}}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	process, err := os_execer.NewExecer().Exec(cmd)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Check for growing memory usage at [.05, .1, .15]s. Then check that the usage is a reasonable minimum value (15MB).
	prevUsage := 0
	for i := 0; i < 3; i++ {
		time.Sleep(50 * time.Millisecond)
		if newUsage, err := process.MemUsage(); err != nil {
			t.Fatalf(err.Error())
		} else if int(newUsage) <= prevUsage {
			t.Fatalf("Expected growing memory, got: %d -> %d @%dms", prevUsage, newUsage, (i+1)*50)
		} else {
			prevUsage = int(newUsage)
		}
	}
	if prevUsage < 15*1024*1024 {
		t.Fatalf("Expected usage to be at least 25MB, was: %dB", prevUsage)
	}

	process.Abort()
}
