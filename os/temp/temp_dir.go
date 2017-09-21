// Package temp makes hierarchical temporary directories
// it offers the same interface as ioutil, but recursive.
// The more you use this to create temporary directories, the fewer places
// we need to change when we want to relocate all our tempfiles.
package temp

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/twitter/scoot/ice"
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

// Create a new directory with a fixed name (this lets us structure our temp files)
func (d *TempDir) FixedDir(name string) (*TempDir, error) {
	if strings.ContainsRune(name, os.PathSeparator) {
		return nil, fmt.Errorf("temp.TempDir.FixedDir: Invalid name %v", name)
	}
	p := path.Join(d.Dir, name)
	if err := os.MkdirAll(p, 0777); err != nil {
		return nil, err
	}
	return &TempDir{p}, nil
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

// TempDirDefault creates a TempDir rooted in the default temp dir
func TempDirDefault() (*TempDir, error) {
	tmpDir, err := ioutil.TempDir("", "scoot-tmp-")
	if err != nil {
		return nil, fmt.Errorf("temp.TempDirDefault: couldn't ioutil.TempDir: %v", err)
	}
	return &TempDir{tmpDir}, err
}

// TempDirHere creates a TempDir rooted in our current working directory
// Side effect: sets environment variable TMPDIR
func TempDirHere() (*TempDir, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("temp.TempDirHere: couldn't getwd: %v", err)
	}

	if err = os.Setenv("TMPDIR", cwd); err != nil {
		return nil, fmt.Errorf("temp.TempDirHere: couldn't setenv %v", err)
	}

	return NewTempDir(cwd, "scoot-tmp-")
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
