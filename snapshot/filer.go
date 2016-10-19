package snapshot

// A Snapshot is a low-level interface offering per-file access to data in a Snapshot.
// This is useful for tools that want one file at a time, or for ScootFS to offer the data.
// Many tools want a higher-level construct: a Filer.
// A Filer lets clients deal with Snapshots as files in the local filesystem.
type Filer interface {
	Checkouter
	Ingester
}

// Checkouter allows reading a Snapshot into the local filesystem.
type Checkouter interface {
	// Checkout checks out the Snapshot identified by id, or an error if it fails.
	Checkout(id string) (Checkout, error)
}

// Checkout represents one checkout of a Snapshot.
// A Checkout is a copy of a Snapshot that lives in the local filesystem at a path.
type Checkout interface {
	// Path in the local filesystem to the Checkout
	Path() string

	// ID of the checked-out Snapshot
	ID() string

	// Releases this Checkout, allowing the Checkouter to clean/recycle this checkout.
	// After Release(), the client may not look at files under Path()
	Release() error
}

type Ingester interface {
	Ingest(path string) (id string, err error)
}
