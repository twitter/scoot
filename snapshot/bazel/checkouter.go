package bazel

import (
	"github.com/twitter/scoot/snapshot"
)

// // Checkouter allows reading a Snapshot into the local filesystem.
// type Checkouter interface {
// 	// Checkout checks out the Snapshot identified by id, or an error if it fails.
// 	Checkout(id string) (Checkout, error)

// 	// Create checkout in a caller controlled dir.
// 	CheckoutAt(id string, dir string) (Checkout, error)
// }

// // Checkout represents one checkout of a Snapshot.
// // A Checkout is a copy of a Snapshot that lives in the local filesystem at a path.
// type Checkout interface {
// 	// Path in the local filesystem to the Checkout
// 	Path() string

// 	// ID of the checked-out Snapshot
// 	ID() string

// 	// Releases this Checkout, allowing the Checkouter to clean/recycle this checkout.
// 	// After Release(), the client may not look at files under Path().
// 	Release() error
// }

func (bf *bzFiler) Checkout(id string) (snapshot.Checkout, error) {
	co, err := bf.checkouter.Checkout(id)
	return co, err
}

func (bf *bzFiler) CheckoutAt(id string, dir string) (snapshot.Checkout, error) {
	co, err := bf.checkouter.CheckoutAt(id, dir)
	return co, err
}

func (bc *bzCheckouter) Checkout(id string) (snapshot.Checkout, error) {
	return &bzCheckout{}, nil
}

func (bc *bzCheckouter) CheckoutAt(id string, dir string) (snapshot.Checkout, error) {
	return &bzCheckout{}, nil
}
