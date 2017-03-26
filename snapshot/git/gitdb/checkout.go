package gitdb

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	snap "github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

func (db *DB) readFileAll(id snap.ID, path string) (string, error) {
	v, err := db.parseID(id)
	if err != nil {
		return "", err
	}

	if err := v.Download(db); err != nil {
		return "", err
	}

	if v.Kind() != kindFSSnapshot {
		return "", fmt.Errorf("can only ReadFileAll from an FSSnapshot, but %v is a %v", id, v.Kind())
	}

	return db.dataRepo.Run("cat-file", "-p", fmt.Sprintf("%s:%s", v.SHA(), path))
}

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
		if id == db.currentSnapID {
			log.Printf("Using cached checkout for id=%s", id)
			return db.dataRepo.Dir(), nil
		}
		path, err := db.checkoutGitCommitSnapshot(v.SHA())
		if err != nil {
			db.currentSnapID = ""
		} else {
			db.currentSnapID = id
		}
		return path, err
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
		// Note: our worktree will often be in detached head state after checkout, but [Twitter] git needs a valid ref to fetch.
		//       we set HEAD to scoot's tmp branch name so subsequent fetch operations, ex: those in stream.go, can succeed.
		if _, err := db.dataRepo.Run("checkout", "-B", tempBranch); err != nil {
			db.workTreeLock.Unlock()
			return err
		}
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

	if err := moveCommit(db.dataRepo, externalRepo, v.SHA()); err != nil {
		return "", err
	}

	return v.SHA(), nil
}

func moveCommit(from *repo.Repository, to *repo.Repository, sha string) error {
	// Strategy: move a commit from 'from' to 'to'
	// first, check if it's in 'to' (if so; skip)
	// delete the ref in 'to'
	// set the ref in 'from'.
	// push from 'from' to 'to'.
	// delete ref in both repos.
	if _, err := to.Run("rev-parse", "--verify", fmt.Sprintf("%s^{commit}", sha)); err == nil {
		return nil
	}

	if _, err := to.Run("update-ref", "-d", tempRef); err != nil {
		return err
	}

	if _, err := from.Run("update-ref", tempRef, sha); err != nil {
		return err
	}

	if _, err := from.Run("push", "-f", to.Dir(), tempRef); err != nil {
		return err
	}

	if _, err := from.Run("update-ref", "-d", tempRef); err != nil {
		return err
	}

	if _, err := to.Run("update-ref", "-d", tempRef); err != nil {
		return err
	}

	return nil
}
