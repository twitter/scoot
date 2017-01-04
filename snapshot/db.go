package snapshot

import (
	"github.com/scootdev/scoot/snapshot/git/repo"
)

// ID identifies a value in DB. Opaque to the client.
type ID string

// DB is the Scoot Database, allowing creation, distribution, and export of Values.
type DB interface {
	// Ingest

	// IngestDir ingests a directory directly.
	// The created value is a Snapshot.
	IngestDir(dir string) (ID, error)

	// IngestGitCommit ingests the commit identified by commitish from ingestRepo
	// commitish may be any string that identifies a commit
	// The created value is a SnapshotWithHistory.
	IngestGitCommit(ingestRepo *repo.Repository, commitish string) (SnapID, error)

	// Operations

	// UnwrapSnapshotHistory unwraps a SnapshotWithHistory and returns a Snapshot.
	// Errors if id does not identify a SnapshotWithHistory.
	SnapshotForRevision(id ID) (ID, error)

	// Distribute

	// Upload makes sure the value id is uploaded, returning an ID that can be used
	// anywhere or an error
	Upload(id ID) (ID, error)

	// Download makes sure the value id is downloaded, returning an ID that can be used
	// on this computer or an error
	Download(id SnapID) (SnapID, error)

	// Export

	// Checkout puts the value identified by id in the local filesystem, returning
	// the path where it lives or an error.
	Checkout(id ID) (path string, err error)

	// ReleaseCheckout releases a path from a previous Checkout. This allows Scoot to reuse
	// the path. Scoot will not touch path after Checkout until ReleaseCheckout.
	ReleaseCheckout(path string) error

	// TODO(dbentley): consider adding utilities to clean up previous Checkouts. E.g., ListCheckouts or ReleaseAll
}
