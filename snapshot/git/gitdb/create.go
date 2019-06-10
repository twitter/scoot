package gitdb

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/snapshot/git/repo"
)

func (db *DB) ingestDirWithRepo(repo *repo.Repository, index, dir string) (snapshot, error) {
	// We ingest a dir using git commands:
	// First, expect new or copied index file.
	// Second, add all the files in the work tree.
	// Third, write the tree.
	// This doesn't create a commit, or otherwise mess with repo state.

	gitEnv := []string{"GIT_INDEX_FILE=" + index, "GIT_WORK_TREE=" + dir}
	log.Infof("Ingesting into %s, env=%s", repo.Dir(), gitEnv)

	// TODO(dbentley): should we use update-index instead of add? Maybe add looks at repo state
	// (e.g., HEAD) and we should just use the lower-level plumbing command?
	_, err := repo.RunExtraEnv(gitEnv, "add", "--all")
	if err != nil {
		return nil, err
	}

	sha, err := repo.RunExtraEnvSha(gitEnv, "write-tree")
	if err != nil {
		return nil, err
	}

	return &localSnapshot{sha: sha, kind: KindFSSnapshot}, nil
}

func (db *DB) ingestDir(dir string) (snapshot, error) {
	indexDir, err := db.tmp.TempDir("git-index")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(indexDir.Dir)
	return db.ingestDirWithRepo(db.dataRepo, filepath.Join(indexDir.Dir, "index"), dir)
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
	indexDir, err := db.tmp.TempDir("git-index")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(indexDir.Dir)

	err = exec.Command("cp", filepath.Join(ingestRepo.Dir(), ".git/index"), indexDir.Dir).Run()
	if err != nil {
		return nil, err
	}

	s, err := db.ingestDirWithRepo(ingestRepo, filepath.Join(indexDir.Dir, "index"), ingestRepo.Dir())
	if err != nil {
		return nil, err
	}

	sha, err := ingestRepo.RunSha("commit-tree", "-p", "HEAD", "-m", "__scoot_commit", s.SHA())
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
