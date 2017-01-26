package gitdb

import (
	"fmt"
	"os"
	"path/filepath"

	snap "github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

// checkout creates a checkout of id.
func (db *DB) checkout(id snap.ID) (path string, err error) {
	defer func() {
		// If we're returning our repo dir, we need to keep the work tree locked.
		// Otherwise, we can unlock it.
		if path != db.dataRepo.Dir() {
			db.workTreeLock.Unlock()
		}
	}()

	v, err := db.parseID(id)
	if err != nil {
		return "", err
	}

	if err := v.Download(db); err != nil {
		return "", err
	}

	switch v.Kind() {
	case kindFSSnapshot:
		// For FSSnapshots, we make a "bare checkout".
		return db.checkoutFSSnapshot(v.SHA())
	case kindGitCommitSnapshot:
		// For GitCommitSnapshot's, we use dataRepo's work tree.
		return db.checkoutGitCommitSnapshot(v.SHA())
	default:
		return "", fmt.Errorf("cannot checkout value kind %v; id %v", v.Kind(), v.ID())
	}
}

// checkoutFSSnapshot creates a new dir with a new index and checks out exactly that tree.
func (db *DB) checkoutFSSnapshot(sha string) (path string, err error) {
	// we don't need the work tree
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

	cmd := db.dataRepo.Command("read-tree", sha)
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

	db.checkouts[coDir.Dir] = true

	return coDir.Dir, nil
}

// checkoutGitCommitSnapshot checks out a commit into our work tree.
// We could use multiple work trees, except our internal git doesn't yet have work-tree support.
// TODO(dbentley): migrate to work-trees.
func (db *DB) checkoutGitCommitSnapshot(sha string) (path string, err error) {
	cmds := [][]string{
		// -d removes directories. -x ignores gitignore and removes everything.
		// -f is force. -f the second time removes directories even if they're git repos themselves
		{"clean", "-f", "-f", "-d", "-x"},
		{"checkout", sha},
	}

	for _, argv := range cmds {
		if _, err := db.dataRepo.Run(argv...); err != nil {
			return "", fmt.Errorf("Unable to run git %v: %v", argv, err)
		}
	}

	return db.dataRepo.Dir(), nil
}

func (db *DB) releaseCheckout(path string) error {
	if path == db.dataRepo.Dir() {
		db.workTreeLock.Unlock()
		return nil
	}

	if exists := db.checkouts[path]; !exists {
		return nil
	}
	err := os.RemoveAll(path)
	if err == nil {
		return nil
	}
	delete(db.checkouts, path)
	return err
}

func (db *DB) exportGitCommit(id snap.ID, externalRepo *repo.Repository) (string, error) {
	v, err := db.parseID(id)
	if err != nil {
		return "", err
	}

	if err := v.Download(db); err != nil {
		return "", err
	}

	if v.Kind() != kindGitCommitSnapshot {
		return "", fmt.Errorf("cannot export non-GitCommitSnapshot %v: %v", id, v.Kind())
	}

	// Strategy: move a commit from data to export
	// first, check if it's in export (if so; skip)
	// delete the ref in the export.
	// set the ref in the data.
	// push from data to export.
	// delete in both repos.

	sha := v.SHA()
	if _, err := externalRepo.Run("rev-parse", "--verify", fmt.Sprintf("%s^{commit}", sha)); err == nil {
		return sha, nil
	}

	if _, err := externalRepo.Run("update-ref", "-d", tempRef); err != nil {
		return "", err
	}

	if _, err := db.dataRepo.Run("update-ref", tempRef); err != nil {
		return "", err
	}

	if _, err := db.dataRepo.Run("push", "-f", externalRepo.Dir(), tempRef); err != nil {
		return "", err
	}

	if _, err := db.dataRepo.Run("update-ref", "-d", tempRef); err != nil {
		return "", err
	}

	if _, err := externalRepo.Run("update-ref", "-d", tempRef); err != nil {
		return "", err
	}

	return sha, nil
}
