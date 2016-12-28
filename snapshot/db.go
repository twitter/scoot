package snapshot

// ID identifies a value in DB
type ID string

// DB is the Scoot Database. It holds Values identified by an ID.
// A Value may be a Snapshot or a Revision.
type DB interface {
	// Ingest

	// IngestDir ingests a directory directly.
	// The created value is a Snapshot.
	IngestDir(dir string) (ID, error)

	// IngestGitCommit ingests the commit identified by commitish from ingestRepo
	// commitish may be any string that identifies a commit
	// The created value is a Revision.
	IngestGitCommit(ingestRepo *repo.Repository, commitish string) (SnapID, error)

	// Operations

	// SnapshotForRevision unwraps a Revision and returns a Snapshot.
	// Errors if id does not identify a Revision.
	SnapshotForRevision(id ID) (ID, error)

	// Distribute

	// Upload makes sure the value id is uploaded, returning an ID that can be used
	// anywhere or an error
	Upload(id ID) (ID, error)

	// Upload makes sure the value id is downloaded, returning an ID that can be used
	// on this computer or an error
	Download(id SnapID) (SnapID, error)

	// Export

	// Checkout puts the value identified by id in the local filesystem, returning
	// the path where it lives or an error.
	Checkout(id ID) (path string, err error)

	// ReleaseCheckout releases a path from a previous Checkout. This allows Scoot to reuse
	// the path. Scoot will not touch path after Checkout until ReleaseCheckout.
	ReleaseCheckout(path string) error
}
