package fake_test

import (
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/fake"
	"testing"
)

func TestSimExec(t *testing.T) {
	assertRun(t, complete(0), 0, "complete 0")
	assertRun(t, complete(1), 0, "complete 1")
	assertRun(t, complete(0), 0, "pause 1", "complete 0")
}

func assertRun(t *testing.T, expected execer.ProcessStatus, minNS int64, argv ...string) {
	ex := fake.NewSimExecer()
	cmd := execer.Command{}
	cmd.Argv = argv
	p, err := ex.Exec(cmd)
	if err != nil {
		t.Fatal("Error running cmd ", err)
	}
	st := p.Wait()
	if st != expected {
		t.Fatalf("Running %v, got %v, expected %v", argv, st, expected)
	}
	// TODO(dbentley): make sure it took some time
}

func complete(exitCode int) execer.ProcessStatus {
	r := execer.ProcessStatus{}
	r.State = execer.COMPLETE
	r.ExitCode = exitCode
	return r
}
