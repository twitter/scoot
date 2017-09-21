package execers

import (
	"testing"

	"github.com/twitter/scoot/runner/execer"
)

func TestInterceptor(t *testing.T) {
	ex1, ex2 := NewSimExecer(), NewSimExecer()
	ex := &InterceptExecer{
		Condition:   StartsWithSimExecer,
		Interceptor: ex1,
		Default:     ex2,
	}

	// block1 will block on ex1; block2 on ex2
	block1 := execer.Command{
		Argv: []string{UseSimExecerArg, "pause", "complete 0"},
	}
	block2 := execer.Command{
		Argv: []string{"pause", "complete 0"},
	}

	go func() {
		ex1.Resume()
	}()
	p, err := ex.Exec(block1)
	if err != nil {
		t.Fatalf("couldn't run block1: %v", err)
	}
	if st := p.Wait(); st.State != execer.COMPLETE {
		t.Fatalf("block1 did not complete: %v", st)
	}

	go func() {
		ex2.Resume()
	}()
	p, err = ex.Exec(block2)
	if err != nil {
		t.Fatalf("couldn't run block2: %v", err)
	}
	if st := p.Wait(); st.State != execer.COMPLETE {
		t.Fatalf("block2 did not complete: %v", st)
	}

}
