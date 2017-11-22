package bzfiler

// // Ingester creates a Snapshot from a path in the local filesystem.
// type Ingester interface {
// 	// Takes an absolute path on the local filesystem.
// 	// The contents of path will be stored in a snapshot which may then be checked out by id.
// 	Ingest(path string) (id string, err error)

// 	// Takes a mapping of source paths to be copied into corresponding destination directories.
// 	// Source paths are absolute, and destination directories are relative to Checkout root.
// 	IngestMap(srcToDest map[string]string) (id string, err error)
// }

func (*BzFiler) Ingest(path string) (string, error) {
	return "", nil
}

func (*BzFiler) IngestMap(srcToDest map[string]string) (string, error) {
	return "", nil
}
