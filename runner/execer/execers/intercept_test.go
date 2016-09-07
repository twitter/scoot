package execers

import (
	"sync"
	"testing"

	"github.com/scootdev/scoot/runner/execer"
)

func TestInterceptor(t *testing.T) {
	var wg1, wg2 sync.WaitGroup
	ex1, ex2 := NewSimExecer(&wg1), NewSimExecer(&wg2)
	ex := &InterceptExecer{
		Condition:   StartsWithSimExecer,
		Interceptor: ex1,
		Default:     ex2,
	}

	// block1 will block on wg1; block2 on wg2
	block1 := execer.Command{
		Argv: []string{UseSimExecerArg, "pause", "complete 0"},
	}
	block2 := execer.Command{
		Argv: []string{"pause", "complete 0"},
	}

	wg2.Add(1)
	p, err := ex.Exec(block1)
	if err != nil {
		t.Fatalf("couldn't run block1: %v", err)
	}
	if st := p.Wait(); st.State != execer.COMPLETE {
		t.Fatalf("block1 did not complete: %v", st)
	}

	wg2.Done()
	wg1.Add(1)

	p, err = ex.Exec(block2)
	if err != nil {
		t.Fatalf("couldn't run block2: %v", err)
	}
	if st := p.Wait(); st.State != execer.COMPLETE {
		t.Fatalf("block2 did not complete: %v", st)
	}

	wg1.Done()
}
