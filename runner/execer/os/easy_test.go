package os_test

import (
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/os"
	"testing"
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
