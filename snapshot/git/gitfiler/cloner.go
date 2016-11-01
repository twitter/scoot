package gitfiler

import (
	"fmt"
	"log"
	"os/exec"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

// A Reference Repository is a way to clone repos locally so that the clone takes less time and disk space.
// By passing --reference <local path> to a git clone, the clone will not copy the whole ODB but instead
// hardlink. This means the clone is much faster and also takes very little extra hard disk space.
// Cf. https://git-scm.com/docs/git-clone

// RepoIniter implementation
// cloner clones a repo using --reference based on a reference repo
type refCloner struct {
	refPool   *RepoPool
	clonesDir *temp.TempDir
}

// Get gets a repo with git clone --reference
func (c *refCloner) Init() (*repo.Repository, error) {
	ref, err := c.refPool.Get()
	defer c.refPool.Release(ref, err)
	if err != nil {
		return nil, err
	}

	cloneDir, err := c.clonesDir.TempDir("clone-")
	if err != nil {
		return nil, err
	}

	// We probably ought to use a separate git dir so that processes can't mess up .git
	cmd := exec.Command("git", "clone", "--reference", ref.Dir(), ref.Dir(), cloneDir.Dir)
	log.Println("gitfiler.refCloner.clone: Cloning", cmd)
	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("gitfiler.refCloner.clone: error cloning: %v", err)
	}
	log.Println("gitfiler.refCloner.clone: Cloning complete")

	return repo.NewRepository(cloneDir.Dir)
}
