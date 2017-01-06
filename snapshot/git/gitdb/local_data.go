package gitdb

import (
	"fmt"
	"os"
	"path/filepath"

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

func parseLocalID(id snapshot.ID, kind valueKind, parts []string) (*localValue, error) {
	if len(parts) != 1 {
		return nil, fmt.Errorf("cannot parse snapshot ID: expected 3 parts in local id %s", id)
	}
	sha := parts[0]
	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &localValue{kind: kind, sha: sha}, nil
}

func (v *localValue) id() snapshot.ID {
	return snapshot.ID(fmt.Sprintf(localIDFmt, localIDText, v.kind, v.sha))
}
func (v *localValue) kindF() valueKind { return v.kind }
func (v *localValue) shaF() string     { return v.sha }

func (v *localValue) download(db *DB) (snapshot.ID, error) {
	// A local value is already present Locally
	return v.id(), nil
}

func (v *localValue) upload(db *DB) (snapshot.ID, error) {
	return "", fmt.Errorf("upload of local scheme value not yet implemented")
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
	return v.id(), nil
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
	return l.id(), nil
}
