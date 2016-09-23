package gitfiler

import (
	"fmt"
	"log"
	"os/exec"
	"sync"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

// cloner clones a repo using --reference based on a reference repo
type cloner struct {
	clonesDir *temp.TempDir

	ref *repo.Repository
	err error

	// wg waits for init
	wg sync.WaitGroup
}

// clone clones a repo
func (c *cloner) clone() (*repo.Repository, error) {
	c.wg.Wait()
	if c.err != nil {
		return nil, c.err
	}
	cloneDir, err := c.clonesDir.TempDir("clone-")
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("git", "clone", "-n", "--reference", c.ref.Dir(), c.ref.Dir(), cloneDir.Dir)
	log.Println("gitfiler.RefRepoCloningCheckouter.clone: Cloning", cmd)
	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("gitfiler.RefRepoCloningCheckouter.clone: error cloning: %v", err)
	}

	return repo.NewRepository(cloneDir.Dir)
}
