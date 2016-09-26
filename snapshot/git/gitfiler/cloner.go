package gitfiler

import (
	"fmt"
	"log"
	"os/exec"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

// cloner clones a repo using --reference based on a reference repo
type refCloner struct {
	refPool   *RepoPool
	clonesDir *temp.TempDir
}

// clone clones a repo
func (c *refCloner) Get() (*repo.Repository, error) {
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

	return repo.NewRepository(cloneDir.Dir)
}
