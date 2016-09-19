package gitfiler

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot/git/repo"
	"github.com/scootdev/scoot/snapshots"
)

// Utilities for Reference Repositories.
// A Reference Repository is a way to clone repos locally so that the clone takes less time and disk space.
// By passing --reference <local path> to a git clone, the clone will not copy the whole ODB but instead
// hardlink. This means the clone is much faster and also takes very little extra hard disk space.
// Cf. https://git-scm.com/docs/git-clone

// RefRepoGetter lets a client get a Repository to use as a Reference Repository.
type RefRepoGetter interface {
	// Get gets the Repository to use as a reference repository.
	Get() (*repo.Repository, error)
}

// RefRepoCloningCheckouter checks out by cloning a Ref Repo.
type RefRepoCloningCheckouter struct {
	getter RefRepoGetter
	tmp    *temp.TempDir

	repo *repo.Repository
	mu   sync.Mutex
}

func NewRefRepoCloningCheckouter(getter RefRepoGetter, tmp *temp.TempDir) *RefRepoCloningCheckouter {
	return &RefRepoCloningCheckouter{
		getter: getter,
		tmp:    tmp,
		repo:   nil,
	}
}

// Checkout checks out id (a raw git sha) into a Checkout.
// It does this by making a new clone (via reference) and checking out id.
func (c *RefRepoCloningCheckouter) Checkout(id string) (snapshots.Checkout, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.repo == nil {
		repository, err := c.getter.Get()
		if err != nil {
			return nil, fmt.Errorf("gitfiler.RefRepoCloningCheckouter.clone: error getting: %v", err)
		}
		c.repo = repository
	}

	clone, err := c.clone()
	if err != nil {
		return nil, err
	}

	// Make sure we clean up if we error
	needToClean := true
	defer func() {
		if needToClean {
			err := os.RemoveAll(clone.Dir())
			log.Println("gitfiler.RefRepoCloningCheckouter.Checkout: had to clean in Checkout", err)
		}
	}()

	if err := clone.Checkout(id); err != nil {
		return nil, fmt.Errorf("gitfiler.RefRepoCloningCheckouter.Checkout: could not git checkout: %v", err)
	}

	needToClean = false
	return &RefRepoCloningCheckout{r: clone, id: id, checkouter: c}, nil
}

func (c *RefRepoCloningCheckouter) clone() (*repo.Repository, error) {
	cloneDir, err := c.tmp.TempDir("clone-")
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("git", "clone", "-n", "--reference", c.repo.Dir(), c.repo.Dir(), cloneDir.Dir)
	log.Println("gitfiler.RefRepoCloningCheckouter.clone: Cloning", cmd)
	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("gitfiler.RefRepoCloningCheckouter.clone: error cloning: %v", err)
	}

	return repo.NewRepository(cloneDir.Dir)
}

type RefRepoCloningCheckout struct {
	r          *repo.Repository
	id         string
	checkouter *RefRepoCloningCheckouter
}

func (c *RefRepoCloningCheckout) Path() string {
	return c.r.Dir()
}

func (c *RefRepoCloningCheckout) ID() string {
	return c.id
}

func (c *RefRepoCloningCheckout) Release() error {
	return os.RemoveAll(c.r.Dir())
}
