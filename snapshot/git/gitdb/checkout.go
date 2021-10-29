package gitdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/errors"
	snap "github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/git/repo"
)

// NOTE we assume in practice this only gets used where we are downloading and reading
// the file to a tempdir, and any underlying file/snapshot is deleted
func (db *DB) readFileAll(id snap.ID, path string) (string, error) {
	v, err := db.parseID(id)
	if err != nil {
		return "", errors.NewError(err, errors.ReadFileAllFailureExitCode)
	}

	tmp, err := ioutil.TempDir(db.tmp, "readFileAll-")
	if err != nil {
		return "", errors.NewError(fmt.Errorf("Failed to create TempDir: %s", err), errors.ReadFileAllFailureExitCode)
	}
	defer os.RemoveAll(tmp)

	r, err := v.DownloadTempRepo(db)
	if err != nil {
		return "", errors.NewError(err, errors.ReadFileAllFailureExitCode)
	}
	defer os.RemoveAll(r.Dir())

	if v.Kind() != KindFSSnapshot {
		return "", errors.NewError(fmt.Errorf("can only ReadFileAll from an FSSnapshot, but %v is a %v", id, v.Kind()), errors.ReadFileAllFailureExitCode)
	}

	s, err := r.Run("cat-file", "-p", fmt.Sprintf("%s:%s", v.SHA(), path))
	if err != nil {
		return "", errors.NewError(err, errors.ReadFileAllFailureExitCode)
	}
	return s, nil
}

// checkout creates a checkout of id.
func (db *DB) checkout(id snap.ID) (path string, err error) {
	defer func() {
		// If we're returning our repo dir, we need to keep the work tree locked, otherwise, we can unlock it.
		// Note: we defer this to capture the various places 'path' is returned.
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
	case KindFSSnapshot:
		// For FSSnapshots, we make a "bare checkout".
		return db.checkoutFSSnapshot(v.SHA())
	case KindGitCommitSnapshot:
		return db.checkoutGitCommitSnapshot(v.SHA())
	default:
		return "", fmt.Errorf("cannot checkout value kind %v; id %v", v.Kind(), v.ID())
	}
}

// checkoutFSSnapshot creates a new dir with a new index and checks out exactly that tree.
func (db *DB) checkoutFSSnapshot(sha string) (path string, err error) {
	// we don't need the work tree
	indexDir, err := ioutil.TempDir(db.tmp, "git-index")
	if err != nil {
		return "", err
	}

	indexFilename := filepath.Join(indexDir, "index")
	defer os.RemoveAll(indexDir)

	coDir, err := ioutil.TempDir("", "checkout")
	if err != nil {
		return "", err
	}

	extraEnv := []string{"GIT_INDEX_FILE=" + indexFilename, "GIT_WORK_TREE=" + coDir}

	_, err = db.dataRepo.RunExtraEnv(extraEnv, "read-tree", sha)
	if err != nil {
		return "", err
	}

	_, err = db.dataRepo.RunExtraEnv(extraEnv, "checkout-index", "-a")
	if err != nil {
		return "", err
	}

	db.checkouts[coDir] = true

	return coDir, nil
}

// checkoutGitCommitSnapshot checks out a commit into our work tree.
// We could use multiple work trees, except our internal git doesn't yet have work-tree support.
func (db *DB) checkoutGitCommitSnapshot(sha string) (path string, err error) {
	// -d removes directories. -x ignores gitignore and removes everything.
	// -f is force. -f the second time removes directories even if they're git repos themselves
	cleanCmd := []string{"clean", "-f", "-f", "-d", "-x"}
	if _, err := db.dataRepo.Run(cleanCmd...); err != nil {
		return "", errors.NewError(fmt.Errorf("Unable to run git %v: %v", cleanCmd, err), errors.CleanFailureExitCode)

	}
	// -f overrides modified files
	// -B resets or creates the named branch when checking out the given sha.
	// Note: our worktree cannot be in detached head state after checkout since [Twitter] git needs a valid ref to fetch.
	//       we use scoot's tmp branch name so here subsequent fetch operations, ex: those in stream.go, can succeed.
	checkoutCmd := []string{"checkout", "-fB", tempCheckoutBranch, sha}
	if _, err := db.dataRepo.Run(checkoutCmd...); err != nil {
		return "", errors.NewError(fmt.Errorf("Unable to run git %v: %v", checkoutCmd, err), errors.CheckoutFailureExitCode)
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

	// TODO - this looks suspicious....
	// why don't we delete the path entry in db.checkouts when we've successfully removed that dir (in if statement
	// up at ln 152)?
	delete(db.checkouts, path)
	return errors.NewError(fmt.Errorf("Error:%v, Releasing checkout path: %v", err, path), errors.ReleaseCheckoutFailureCode)
}

func (db *DB) exportGitCommit(id snap.ID, externalRepo *repo.Repository) (string, error) {
	v, err := db.parseID(id)
	if err != nil {
		return "", errors.NewError(err, errors.ExportGitCommitFailureExitCode)
	}

	if err := v.Download(db); err != nil {
		return "", errors.NewError(err, errors.ExportGitCommitFailureExitCode)
	}

	if v.Kind() != KindGitCommitSnapshot {
		return "", errors.NewError(fmt.Errorf("cannot export non-GitCommitSnapshot %v: %v", id, v.Kind()), errors.ExportGitCommitFailureExitCode)
	}

	if err := moveCommit(db.dataRepo, externalRepo, v.SHA()); err != nil {
		return "", errors.NewError(err, errors.ExportGitCommitFailureExitCode)
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
	if from.Dir() == to.Dir() {
		return nil
	}

	if _, err := to.Run("rev-parse", "--verify", fmt.Sprintf("%s^{commit}", sha)); err == nil {
		return nil
	}
	log.Infof("Could not find commit=%s, continuing with moveCommit()", sha)

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
