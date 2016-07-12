package fake_test

import (
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/fake"
	"sync"
	"testing"
)

func TestSimExec(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	ex := fake.NewSimExecer(&wg)
	assertRun(ex, t, complete(0), "complete 0")
	assertRun(ex, t, complete(1), "complete 1")
	argv := []string{"pause", "complete 0"}
	p := assertStart(ex, t, argv...)
	wg.Done()
	assertStatus(t, complete(0), p, argv...)
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
