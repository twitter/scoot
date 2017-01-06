package gitdb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/scootdev/scoot/snapshot"
)

// checkout creates a checkout of id.
func (db *DB) checkout(id snapshot.ID) (path string, err error) {

	defer func() {
		// If we're returning our repo dir, we need to keep the work tree locked.
		// Otherwise, we can unlock it.
		if path != "" && path != db.dataRepo.Dir() {
			db.workTreeLock.Unlock()
		}
	}()

	v, err := parseID(id)
	if err != nil {
		return "", err
	}

	switch v.kindF() {
	case kindSnapshot:
		// For snapshots, we make a "bare checkout".
		return db.checkoutSnapshot(v.shaF())
	case kindSnapshotWithHistory:
		// For snapshotWithHistory's, we use dataRepo's work tree.
		return db.checkoutSnapshotWithHistory(v.shaF())
	default:
		return "", fmt.Errorf("cannot checkout value kind %v", v.kindF())
	}
}

// checkoutSnapshot creates a new dir with a new index and checks out exactly that tree.
func (db *DB) checkoutSnapshot(sha string) (path string, err error) {
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

// checkoutSnapshotWithHistory checks out a commit into our work tree.
// We could use multiple work trees, except our git doens't yet have work-tree support.
// TODO(dbentley): migrate to work-trees.
func (db *DB) checkoutSnapshotWithHistory(sha string) (path string, err error) {
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
