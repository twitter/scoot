package gitdb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

// valueKind describes the kind of a Value: is it a Snapshot or a SnapshotWithHistory?
// kind instead of type because type is a keyword
type valueKind int

const (
	kindSnapshot valueKind = iota
	kindSnapshotWithHistory
)

// MakeDB makes a gitdb.DB that uses dataRepo for data and tmp for temporary directories
func MakeDB(dataRepo *repo.Repository, tmp *temp.TempDir) *DB {
	return &DB{
		dataRepo: dataRepo,
		tmp:      tmp,
	}
}

// DB stores its data in a Git Repo
type DB struct {
	dataRepo *repo.Repository
	tmp      *temp.TempDir
}

// IngestDir ingests a directory directly.
// The created value is a Snapshot.
func (db *DB) IngestDir(dir string) (snapshot.ID, error) {
	// We ingest a dir using git commands:
	// First, create a new index file.
	// Second, add all the files in the work tree.
	// Third, write the tree.
	// This doesn't create a commit, or otherwise mess with repo state.
	indexDir, err := db.tmp.TempDir("git-index")
	if err != nil {
		return "", err
	}

	indexFilename := filepath.Join(indexDir.Dir, "index")
	defer os.RemoveAll(indexDir.Dir)

	extraEnv := []string{"GIT_INDEX_FILE=" + indexFilename, "GIT_WORK_TREE=" + dir}

	// TODO(dbentley): should we use update-index instead of add? Maybe add looks at repo state
	// (e.g., HEAD) and we should just use the lower-level plumbing command?
	cmd := db.dataRepo.Command("add", ".")
	cmd.Env = append(cmd.Env, extraEnv...)
	_, err = db.dataRepo.RunCmd(cmd)
	if err != nil {
		return "", err
	}

	cmd = db.dataRepo.Command("write-tree")
	cmd.Env = append(cmd.Env, extraEnv...)
	sha, err := db.dataRepo.RunCmd(cmd)
	if err != nil {
		return "", err
	}
	sha, err = repo.ValidateSha(sha)
	if err != nil {
		return "", err
	}

	v := &localValue{sha: sha, kind: kindSnapshot}
	return v.ID(), nil
}

// IngestGitCommit ingests the commit identified by commitish from ingestRepo
// commitish may be any string that identifies a commit
// The created value is a SnapshotWithHistory.
func (db *DB) IngestGitCommit(ingestRepo *repo.Repository, commitish string) (snapshot.ID, error) {
	return "", fmt.Errorf("not yet implemented")
}

// Operations

// UnwrapSnapshotHistory unwraps a SnapshotWithHistory and returns a Snapshot ID.
// Errors if id does not identify a SnapshotWithHistory.
func (db *DB) UnwrapSnapshotHistory(id snapshot.ID) (snapshot.ID, error) {
	return "", fmt.Errorf("not yet implemented")
}

// Distribute

// Upload makes sure the value id is uploaded, returning an ID that can be used
// anywhere or an error
func (db *DB) Upload(id snapshot.ID) (snapshot.ID, error) {
	return "", fmt.Errorf("not yet implemented")
}

// Download makes sure the value id is downloaded, returning an ID that can be used
// on this computer or an error
func (db *DB) Download(id snapshot.ID) (snapshot.ID, error) {
	return "", fmt.Errorf("not yet implemented")
}

// Export

// Checkout puts the value identified by id in the local filesystem, returning
// the path where it lives or an error.
func (db *DB) Checkout(id snapshot.ID) (path string, err error) {
	// Checkout creates a new dir with a new index and checks out exactly that tree.
	v, err := parseID(id)
	if err != nil {
		return "", err
	}

	indexDir, err := db.tmp.TempDir("git-index")
	if err != nil {
		return "", err
	}

	indexFilename := filepath.Join(indexDir.Dir, "index")
	defer os.RemoveAll(indexDir.Dir)

	coDir, err := db.tmp.TempDir("checkout")
	if err != nil {
		return "", err
	}

	extraEnv := []string{"GIT_INDEX_FILE=" + indexFilename, "GIT_WORK_TREE=" + coDir.Dir}

	cmd := db.dataRepo.Command("read-tree", v.sha)
	cmd.Env = append(cmd.Env, extraEnv...)
	_, err = db.dataRepo.RunCmd(cmd)
	if err != nil {
		return "", err
	}

	cmd = db.dataRepo.Command("checkout-index", "-a")
	cmd.Env = append(cmd.Env, extraEnv...)
	_, err = db.dataRepo.RunCmd(cmd)
	if err != nil {
		return "", err
	}

	return coDir.Dir, nil
}

// ReleaseCheckout releases a path from a previous Checkout. This allows Scoot to reuse
// the path. Scoot will not touch path after Checkout until ReleaseCheckout.
func (db *DB) ReleaseCheckout(path string) error {
	// TODO(dbentley): track checkouts, and clean if we can now
	return nil
}
