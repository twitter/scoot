package bazel

import (
	"os"

	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

// Satisfies snapshot.Checkout
type bzCheckout struct {
	dir           string
	keepOnRelease bool
	remoteexecution.Digest
}

func (bc *bzCheckout) Path() string {
	return bc.dir
}

func (bc *bzCheckout) ID() string {
	return generateId(bc.GetHash(), bc.GetSizeBytes())
}

func (bc *bzCheckout) Release() error {
	if bc.keepOnRelease {
		return nil
	}
	return os.Remove(bc.Path())
}
