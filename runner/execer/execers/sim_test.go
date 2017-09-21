package execers

import (
	"bytes"
	"testing"

	"github.com/twitter/scoot/runner/execer"
)

func TestSimExec(t *testing.T) {
	ex := NewSimExecer()
	assertRun(ex, t, complete(0), "complete 0")
	assertRun(ex, t, complete(1), "complete 1")
	assertRun(ex, t, complete(0), "sleep 1", "complete 0")
	assertRun(ex, t, complete(0), "#this is a comment", "complete 0")
	argv := []string{"pause", "complete 0"}
	p := assertStart(ex, t, argv...)
	ex.Resume()
	assertStatus(t, complete(0), p, argv...)
}

func TestOutput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	expectedStdout, expectedStderr := "foo\n", "bar\n"
	cmd := execer.Command{
		Argv:   []string{"stdout " + expectedStdout, "stderr " + expectedStderr, "complete 0"},
		Stdout: &stdout,
		Stderr: &stderr,
	}

	ex := NewSimExecer()
	p, err := ex.Exec(cmd)
	if err != nil {
		t.Fatal("Error running cmd", err)
	}
	st := p.Wait()
	if st != complete(0) {
		t.Fatalf("got status %v; expected %v", st, complete(0))
	}
	if stdout.String() != expectedStdout {
		t.Fatalf("got stdout %v; expected %v", stdout.String(), expectedStdout)
	}
	if stderr.String() != expectedStderr {
		t.Fatalf("got stderr %v; expected %v", stderr.String(), expectedStderr)
	}
}

func assertRun(ex execer.Execer, t *testing.T, expected execer.ProcessStatus, argv ...string) {
	p := assertStart(ex, t, argv...)
	assertStatus(t, expected, p, argv...)
}

func assertStart(ex execer.Execer, t *testing.T, argv ...string) execer.Process {
	cmd := execer.Command{}
	cmd.Argv = argv
	p, err := ex.Exec(cmd)
	if err != nil {
		t.Fatal("Error running cmd ", err)
	}
	return p
}

func assertStatus(t *testing.T, expected execer.ProcessStatus, p execer.Process, argv ...string) {
	st := p.Wait()
	if st != expected {
		t.Fatalf("Running %v, got %v, expected %v", argv, st, expected)
	}
}

func complete(exitCode int) execer.ProcessStatus {
	r := execer.ProcessStatus{}
	r.State = execer.COMPLETE
	r.ExitCode = exitCode
	return r
}
