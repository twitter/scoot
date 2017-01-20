package bundlestore

import (
	"io"
)

// Read-only operations on store, limited for now to a couple essential functions.
type StoreRead interface {
	// Check if the bundle exists. Not guaranteed to be any cheaper than actually reading the bundle.
	Exists(name string) (bool, error)

	// Open the bundle for streaming read. It is the caller's responsibility to call Close().
	OpenForRead(name string) (io.ReadCloser, error)
}

// Write operations on store, limited to a one-shot writing operation since bundles are immutable.
type StoreWrite interface {
	// Does a streaming write of the given bundle. There is no concept of partial writes (partial=failed).
	Write(name string, data io.Reader) error
}

// Combines read and write operations on store. This is what most of the code will use.
type Store interface {
	StoreRead
	StoreWrite
}
