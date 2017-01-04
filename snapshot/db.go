package snapshot

import (
	"github.com/scootdev/scoot/snapshot/git/repo"
)

// ID identifies a Value in DB. (Cf. doc.go for explanation of Value) Opaque to the client.
type ID string

// DB is the Scoot Database, allowing creation, distribution, and export of Values.
type DB interface {
	// Ingest

	// IngestDir ingests a directory directly.
	// The created Value is a Snapshot.
	IngestDir(dir string) (ID, error)

	// IngestGitCommit ingests the commit identified by commitish from ingestRepo
	// commitish may be any string that identifies a commit
	// The created Value is a SnapshotWithHistory.
	IngestGitCommit(ingestRepo *repo.Repository, commitish string) (ID, error)

	// Operations

	// UnwrapSnapshotHistory unwraps a SnapshotWithHistory and returns a Snapshot ID.
	// Errors if id does not identify a SnapshotWithHistory.
	UnwrapSnapshotHistory(id ID) (ID, error)

	// Distribute

	// Upload makes sure the Value id is uploaded, returning an ID that can be used
	// anywhere or an error
	Upload(id ID) (ID, error)

	// Download makes sure the Value id is downloaded, returning an ID that can be used
	// on this computer or an error
	Download(id ID) (ID, error)

	// Export

	// Checkout puts the Value identified by id in the local filesystem, returning
	// the path where it lives or an error.
	// TODO(dbentley): should this download if necessary?
	Checkout(id ID) (path string, err error)

	// ReleaseCheckout releases a path from a previous Checkout. This allows Scoot to reuse
	// the path. Scoot will not touch path after Checkout until ReleaseCheckout.
	ReleaseCheckout(path string) error

	// TODO(dbentley): consider adding utilities to clean up previous Checkouts. E.g., ListCheckouts or ReleaseAll
}
