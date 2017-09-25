package snapshot

import (
	"github.com/twitter/scoot/snapshot/git/repo"
)

// ID identifies a Snapshot in DB. (Cf. doc.go for explanation of Snapshot) Opaque to the client.
type ID string

// DB may require some form of initialization, in which case this chan should be provided by the db impl.
type InitDoneCh <-chan error

// Creator allows creating new Snapshots.
type Creator interface {
	// Ingest

	// IngestDir ingests a directory directly.
	// Creates an FSSnapshot whose contents are the same as the directory in the
	// local filesystem at the path identified by dir.
	// TODO(dbentley): define behavior on non-{file,directory} filetypes encountered
	// in dir, e.g. block devices or symlinks
	IngestDir(dir string) (ID, error)

	// IngestGitCommit ingests the commit identified by commitish from ingestRepo
	// commitish may be any string that identifies a commit
	// Creates a GitCommitSnapshot that mirrors the ingested commit.
	IngestGitCommit(ingestRepo *repo.Repository, commitish string) (ID, error)

	// IngestGitWorkingDir ingests HEAD + working dir modifications from ingestRepo.
	// Creates a GitCommitSnapshot that mirrors the ingested commit.
	IngestGitWorkingDir(ingestRepo *repo.Repository) (ID, error)
}

// Reader allows reading data from existing Snapshots
type Reader interface {
	// ReadFileAll reads the contents of the file path in FSSnapshot ID, or errors
	ReadFileAll(id ID, path string) ([]byte, error)

	// Checkout puts the Snapshot identified by id in the local filesystem, returning
	// the path where it lives or an error.
	// TODO(dbentley): should we have separate methods based on the kind of Snapshot?
	Checkout(id ID) (path string, err error)

	// ReleaseCheckout releases a path from a previous Checkout. This allows Scoot to reuse
	// the path. Scoot will not touch path after Checkout until ReleaseCheckout.
	ReleaseCheckout(path string) error

	// TODO(dbentley): consider adding utilities to clean up previous Checkouts. E.g., ListCheckouts or ReleaseAll

	// ExportGitCommit puts the GitCommitSnapshot identified by id into exportRepo,
	// returning the sha of the exported commit.
	ExportGitCommit(id ID, exportRepo *repo.Repository) (commit string, err error)
}

// DB is the full read-write Snapshot Database, allowing creation and reading of Snapshots,
// and updating of the underlying DB resource.
type DB interface {
	Creator
	Reader
	Updater
}
