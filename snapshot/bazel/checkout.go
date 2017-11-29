package bazel

import (
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

// Satisfies snapshot.Checkout
type bzCheckout struct {
	dir string
	remoteexecution.Digest
}

func (bc *bzCheckout) Path() string {
	return bc.dir
}

func (bc *bzCheckout) ID() string {
	return bc.Hash
}

func (bc *bzCheckout) Release() error {
	// NOT YET IMPLEMENTED
	return nil
}
