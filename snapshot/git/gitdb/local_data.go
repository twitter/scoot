package gitdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

const localIDText = "local"
const localIDFmt = "%s-%s-%s"

// localValue holds a reference to a value that is in the local DB
type localValue struct {
	sha  string
	kind valueKind
}

func (v *localValue) ID() snapshot.ID {
	return snapshot.ID(fmt.Sprintf(localIDFmt, localIDText, v.kind, v.sha))
}

// parseID parses ID into a localValue
func parseID(id snapshot.ID) (*localValue, error) {
	if id == "" {
		return nil, fmt.Errorf("empty snapshot ID")
	}
	parts := strings.Split(string(id), "-")
	if len(parts) != 3 {
		return nil, fmt.Errorf("did not scan 3 items from %s", id)
	}
	scheme, kind, sha := parts[0], valueKind(parts[1]), parts[2]
	if scheme != localIDText {
		return nil, fmt.Errorf("invalid scheme: %s", scheme)
	}

	if !kinds[kind] {
		return nil, fmt.Errorf("invalid kind: %s", kind)
	}

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &localValue{kind: kind, sha: sha}, nil
}

// validSha checks if sha is valid
func validSha(sha string) error {
	if len(sha) != 40 {
		return fmt.Errorf("sha not 40 characters: %s", sha)
	}
	// TODO(dbentley): check that it's hexadecimal?
	return nil
}

func (db *DB) ingestDir(dir string) (snapshot.ID, error) {
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
	sha, err := db.dataRepo.RunCmdSha(cmd)
	if err != nil {
		return "", err
	}

	v := &localValue{sha: sha, kind: kindSnapshot}
	return v.ID(), nil
}

const tempRef = "refs/heads/scoot/__temp_for_writing"

func (db *DB) ingestGitCommit(ingestRepo *repo.Repository, commitish string) (snapshot.ID, error) {
	sha, err := ingestRepo.RunSha("rev-parse", "--verify", fmt.Sprintf("%s^{commit}", commitish))
	if err != nil {
		return "", fmt.Errorf("not a valid commit: %s, %v", commitish, err)
	}

	// Strategy:
	// trying to move a commit from ingest to data
	// delete the ref in the data.
	// set the ref in the ingest.
	// push from ingest to data.
	// delete in both repos.
	// TODO(dbentley): we could check if sha exists in our repo before ingesting

	if _, err := db.dataRepo.Run("update-ref", "-d", tempRef); err != nil {
		return "", err
	}

	if _, err := ingestRepo.Run("update-ref", tempRef, sha); err != nil {
		return "", err
	}

	if _, err := ingestRepo.Run("push", "-f", db.dataRepo.Dir(), tempRef); err != nil {
		return "", err
	}

	if _, err := ingestRepo.Run("update-ref", "-d", tempRef); err != nil {
		return "", err
	}

	if _, err := db.dataRepo.Run("update-ref", "-d", tempRef); err != nil {
		return "", err
	}

	l := &localValue{sha: sha, kind: kindSnapshotWithHistory}
	return l.ID(), nil
}

// checkout creates a checkout of id.
func (db *DB) checkout(id snapshot.ID) (path string, err error) {

	defer func() {
		// If we're our repo dir, we need to keep the work tree locked.
		// Otherwise, we can unlock it.
		if path != "" && path != db.dataRepo.Dir() {
			db.workTreeLock.Unlock()
		}
	}()

	v, err := parseID(id)
	if err != nil {
		return "", err
	}

	// We use different strategies depending on the kind of snapshot.
	switch v.kind {
	case kindSnapshot:
		// For snapshots, we make a "bare checkout".
		return db.checkoutSnapshot(v.sha)
	case kindSnapshotWithHistory:
		// For snapshotWithHistory's, we use dataRepo's work tree.
		return db.checkoutSnapshotWithHistory(v.sha)
	default:
		return "", fmt.Errorf("cannot checkout value kind %v", v.kind)
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
