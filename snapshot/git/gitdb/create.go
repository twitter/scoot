package gitdb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/scootdev/scoot/snapshot/git/repo"
)

func (db *DB) ingestDirWithRepo(repo *repo.Repository, dir string) (snapshot, error) {
	// We ingest a dir using git commands:
	// First, create a new index file.
	// Second, add all the files in the work tree.
	// Third, write the tree.
	// This doesn't create a commit, or otherwise mess with repo state.
	indexDir, err := db.tmp.TempDir("git-index")
	if err != nil {
		return nil, err
	}

	indexFilename := filepath.Join(indexDir.Dir, "index")
	defer os.RemoveAll(indexDir.Dir)

	env := append(os.Environ(), "GIT_INDEX_FILE="+indexFilename, "GIT_WORK_TREE="+dir)

	// TODO(dbentley): should we use update-index instead of add? Maybe add looks at repo state
	// (e.g., HEAD) and we should just use the lower-level plumbing command?
	cmd := repo.Command("add", "--all")
	cmd.Env = env
	_, err = repo.RunCmd(cmd)
	if err != nil {
		return nil, err
	}

	cmd = repo.Command("write-tree")
	cmd.Env = env
	sha, err := repo.RunCmdSha(cmd)
	if err != nil {
		return nil, err
	}

	return &localSnapshot{sha: sha, kind: KindFSSnapshot}, nil
}

func (db *DB) ingestDir(dir string) (snapshot, error) {
	return db.ingestDirWithRepo(db.dataRepo, dir)
}

const tempBranch = "scoot/__temp_for_writing"
const tempCheckoutBranch = "scoot/__temp_for_checkout"
const tempRef = "refs/heads/" + tempBranch

func (db *DB) ingestGitCommit(ingestRepo *repo.Repository, commitish string) (snapshot, error) {
	sha, err := ingestRepo.RunSha("rev-parse", "--verify", fmt.Sprintf("%s^{commit}", commitish))
	if err != nil {
		return nil, fmt.Errorf("not a valid commit: %s, %v", commitish, err)
	}

	if err := db.shaPresent(sha); err == nil {
		return &localSnapshot{sha: sha, kind: KindGitCommitSnapshot}, nil
	}

	if err := moveCommit(ingestRepo, db.dataRepo, sha); err != nil {
		return nil, err
	}

	return &localSnapshot{sha: sha, kind: KindGitCommitSnapshot}, nil
}

func (db *DB) ingestGitWorkingDir(ingestRepo *repo.Repository) (snapshot, error) {
	s, err := db.ingestDirWithRepo(ingestRepo, ingestRepo.Dir())
	if err != nil {
		return nil, err
	}

	cmd := ingestRepo.Command("commit-tree", "-p", "HEAD", "-m", "__scoot_commit", s.SHA())
	sha, err := ingestRepo.RunCmdSha(cmd)
	if err != nil {
		return nil, err
	}

	if err := moveCommit(ingestRepo, db.dataRepo, sha); err != nil {
		return nil, err
	}

	return &localSnapshot{sha: sha, kind: KindGitCommitSnapshot}, nil
}

func (db *DB) shaPresent(sha string) error {
	_, err := db.dataRepo.Run("rev-parse", "--verify", sha+"^{object}")
	return err
}
