package gitdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/scootdev/scoot/snapshot"
)

const localIDText = "local"
const localIDFmt = "%s-%s-%s"

// localValue holds a reference to a value that is in the local DB
type localValue struct {
	sha  string
	kind valueKind
}

const (
	snapshotIDText            = "snap"
	snapshotWithHistoryIDText = "swh"
)

var kindToIDText = map[valueKind]string{
	kindSnapshot:            snapshotIDText,
	kindSnapshotWithHistory: snapshotWithHistoryIDText,
}

var kindIDTextToKind = map[string]valueKind{
	snapshotIDText:            kindSnapshot,
	snapshotWithHistoryIDText: kindSnapshotWithHistory,
}

func (v *localValue) ID() snapshot.ID {
	return snapshot.ID(fmt.Sprintf(localIDFmt, localIDText, kindToIDText[v.kind], v.sha))
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
	scheme, kindText, sha := parts[0], parts[1], parts[2]
	if scheme != localIDText {
		return nil, fmt.Errorf("invalid scheme: %s", scheme)
	}

	kind, ok := kindIDTextToKind[kindText]
	if !ok {
		return nil, fmt.Errorf("invalid kind: %s", kindText)
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

func (db *DB) checkout(id snapshot.ID) (path string, err error) {
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

	db.checkouts[coDir.Dir] = true

	return coDir.Dir, nil
}

func (db *DB) releaseCheckout(path string) error {
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
