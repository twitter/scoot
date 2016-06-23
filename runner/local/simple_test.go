package local_test

import (
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/fake"
	"github.com/scootdev/scoot/runner/local"
	"testing"
)

func TestFoo(t *testing.T) {
	exec := fake.NewSimExecer()
	r := local.NewSimpleRunner(exec)
	cmd := &runner.Command{}
	cmd.Argv = []string{"complete 0"}
	status, err := r.Run(cmd)
	if err != nil {
		t.Fatal("Couldn't run: ", err, cmd.Argv)
	}
}
