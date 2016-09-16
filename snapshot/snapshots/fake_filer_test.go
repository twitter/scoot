package snapshots

import (
	"sync"
	"testing"

	"github.com/scootdev/scoot/snapshot"
)

type wgIniter struct {
	wg sync.WaitGroup
}

func (i *wgIniter) Init() error {
	i.wg.Wait()
	return nil
}

func TestIniting(t *testing.T) {
	i := &wgIniter{}
	i.wg.Add(1)

	c := MakeInitingCheckouter("/fake/path", i)
	resultCh := make(chan snapshot.Checkout)
	go func() {
		result, _ := c.Checkout("foo")
		resultCh <- result
	}()

	select {
	case <-resultCh:
		t.Fatalf("should't be able to checkout until init is done")
	default:
	}

	i.wg.Done()
	checkout := <-resultCh
	if checkout.Path() != "/fake/path" {
		t.Fatalf("surprising path: %q", checkout.Path())
	}
}
