// temp makes hierarchical temporary directories
// it offers the same interface as ioutil, but recursive.
// The more you use this to create temporary directories, the fewer places
// we need to change when we want to relocate all our tempfiles.
package temp

import (
	"io/ioutil"
	"os"
)

// Create a new TempDir in directory dir with prefix string.
func NewTempDir(dir, prefix string) (*TempDir, error) {
	p, err := ioutil.TempDir(dir, prefix)
	if err != nil {
		return nil, err
	}
	return &TempDir{Dir: p}, nil
}

// TempDir is a temporary directory, that may live under other temporary directories.
type TempDir struct {
	Dir string
}

// Create a new temporary directory under d
func (d *TempDir) TempDir(prefix string) (*TempDir, error) {
	p, err := ioutil.TempDir(d.Dir, prefix)
	if err != nil {
		return nil, err
	}
	return &TempDir{Dir: p}, nil
}

// Create a new temporary file under d
func (d *TempDir) TempFile(prefix string) (*os.File, error) {
	return ioutil.TempFile(d.Dir, prefix)
}
