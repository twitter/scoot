package os_test

import (
	"bytes"
	"testing"

	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/os"
)

func TestAll(t *testing.T) {
	exer := os.NewExecer()

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
	exer := os.NewExecer()

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
