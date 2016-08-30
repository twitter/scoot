package fake

import (
	"sync"

	"github.com/scootdev/scoot/snapshots"
)

func MakeInvalidCheckouter() snapshots.Checkouter {
	return &noopCheckouter{path: "/path/is/invalid"}
}

type noopCheckouter struct {
	path string
}

func (c *noopCheckouter) Checkout(id string) (snapshots.Checkout, error) {
	return &staticCheckout{
		path: c.path,
		id:   id,
	}, nil
}

type staticCheckout struct {
	path string
	id   string
}

func (c *staticCheckout) Path() string {
	return c.path
}

func (c *staticCheckout) ID() string {
	return c.id
}

func (c *staticCheckout) Release() error {
	return nil
}

// Initer will do something once, e.g., clone a git repo (that might be expensive)
type Initer interface {
	Init() error
}

func MakeInitingCheckouter(path string, initer Initer) snapshots.Checkouter {
	r := &initingCheckouter{path: path}
	// Start the Initer as soon as we know we'll need to
	r.wg.Add(1)
	go func() {
		initer.Init()
		r.wg.Done()
	}()
	return r
}

// initingCheckout waits for an Initer to be done Initing before checking out.
type initingCheckouter struct {
	wg   sync.WaitGroup
	path string
}

func (c *initingCheckouter) Checkout(id string) (snapshots.Checkout, error) {
	c.wg.Wait()
	return &staticCheckout{
		path: c.path,
		id:   id,
	}, nil
}
