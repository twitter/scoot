// Package temp makes hierarchical temporary directories
// it offers the same interface as ioutil, but recursive.
// The more you use this to create temporary directories, the fewer places
// we need to change when we want to relocate all our tempfiles.
package temp

import (
	"io/ioutil"

	"github.com/twitter/scoot/ice"
)

// TempDirDefault creates a TempDir rooted in the default temp dir
func TempDirDefault() (string, error) {
	return ioutil.TempDir("", "scoot-tmp-")
}

// Module returns a Module that will use a TempDir in $TMPDIR
func Module() ice.Module {
	return module{}
}

type module struct{}

// Install installs functions to use a TempDir in $TMPDIR
func (m module) Install(b *ice.MagicBag) {
	b.Put(TempDirDefault)
}
