package min

import (
	"github.com/twitter/scoot/fuse"
)

// The interface for our filesystem.
// TODO(dbentley): I think having this as an interface may be too much. What if we just
// have the concrete implementation that sits atop the snapshots interface?
// That would remove one level of pluggability that we never plan to use.

type FS interface {
	Root() (Node, error)
}

type Node interface {
	Attr() (fuse.Attr, error)
	Readlink() (string, error)
	Lookup(name string) (Node, error)
	Open() (Handle, error)
}

type Handle interface {
	Release() error
	ReadAt(data []byte, offset int64) (int, error)
	ReadDirAll() ([]fuse.Dirent, error)
}
