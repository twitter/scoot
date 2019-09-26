package snapshots

import (
	"github.com/twitter/scoot/apiserver/snapshot"
	"github.com/twitter/scoot/common/os/temp"
)

// Create a Checkouter that essentially does nothing, based on a static path
func MakeInvalidCheckouter() snapshot.Checkouter {
	return &noopCheckouter{path: "/path/is/invalid"}
}

func MakeNoopCheckouter(path string) snapshot.Checkouter {
	return &noopCheckouter{path: path}
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

func (c *noopCheckouter) CancelCheckout() error {
	return nil
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

func (c *tempCheckouter) CancelCheckout() error {
	return nil
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
