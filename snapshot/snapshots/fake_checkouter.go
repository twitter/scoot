package snapshots

import (
	"sync"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot"
)

func MakeInvalidCheckouter() snapshot.Checkouter {
	return &noopCheckouter{path: "/path/is/invalid"}
}

type noopCheckouter struct {
	path string
}

func (c *noopCheckouter) Checkout(id string) (snapshot.Checkout, error) {
	return c.CheckoutAt(id, c.path)
}

func (c *noopCheckouter) CheckoutAt(id string, dir string) (snapshot.Checkout, error) {
	return &staticCheckout{
		path: dir,
		id:   id,
	}, nil
}

// MakeTempCheckouter creates a new Checkouter that always checks out by creating a new, empty temp dir
func MakeTempCheckouter(tmp *temp.TempDir) snapshot.Checkouter {
	return &tempCheckouter{tmp: tmp}
}

type tempCheckouter struct {
	tmp *temp.TempDir
}

func (c *tempCheckouter) Checkout(id string) (snapshot.Checkout, error) {
	t, err := c.tmp.TempDir("checkout-")
	if err != nil {
		return nil, err
	}
	return c.CheckoutAt(id, t.Dir)
}

func (c *tempCheckouter) CheckoutAt(id string, dir string) (snapshot.Checkout, error) {
	return &staticCheckout{
		path: dir,
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

func MakeInitingCheckouter(path string, initer Initer) snapshot.Checkouter {
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

func (c *initingCheckouter) Checkout(id string) (snapshot.Checkout, error) {
	return c.CheckoutAt(id, c.path)
}

func (c *initingCheckouter) CheckoutAt(id string, dir string) (snapshot.Checkout, error) {
	c.wg.Wait()
	return &staticCheckout{
		path: dir,
		id:   id,
	}, nil
}
