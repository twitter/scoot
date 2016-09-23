package gitfiler

import (
	"fmt"
	"io/ioutil"
	"path"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
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
	clones *repoPool
}

func NewRefRepoCloningCheckouter(getter RefRepoGetter, clonesDir *temp.TempDir, doneCh chan struct{}) *RefRepoCloningCheckouter {
	cloner := &cloner{clonesDir: clonesDir}
	cloner.wg.Add(1)
	go func() {
		cloner.ref, cloner.err = getter.Get()
		cloner.wg.Done()
	}()

	r := &RefRepoCloningCheckouter{
		clones: newRepoPool(cloner, doneCh),
	}

	// Find existing clones and reuse them
	go func() {
		fis, _ := ioutil.ReadDir(clonesDir.Dir)
		for _, fi := range fis {
			// TODO(dbentley): maybe we should check that these clones are in fact clones
			// of the reference repo? Using some kind of git commands to determine its upstream?
			clone, err := repo.NewRepository(path.Join(clonesDir.Dir, fi.Name()))
			if err != nil {
				continue
			}
			r.clones.Release(clone)
		}
	}()

	return r
}

// Checkout checks out id (a raw git sha) into a Checkout.
// It does this by making a new clone (via reference) and checking out id.
// TODO(dbentley): if id is not found because it is present in upstream repos but not here,
// we should fetch it and check it out.
func (c *RefRepoCloningCheckouter) Checkout(id string) (co snapshot.Checkout, err error) {
	clone, err := c.clones.Reserve()
	if err != nil {
		return nil, err
	}

	// release if we aren't using it
	defer func() {
		if err != nil || recover() != nil {
			c.clones.Release(clone)
		}
	}()

	// -d removes directories. -x ignores gitignore and removes everything.
	// -f is force. -f the second time removes directories even if they're git repos themselves
	cmds := [][]string{
		{"clean", "-f", "-f", "-d", "-x"},
		{"checkout", id},
	}

	for _, argv := range cmds {
		if _, err := clone.Run(argv...); err != nil {
			return nil, fmt.Errorf("gitfiler.RefRepoCloningCheckouter.Checkout: %v", err)
		}
	}
	return &RefRepoCloningCheckout{repo: clone, id: id, pool: c.clones}, nil
}

// RefRepoCloningCheckout holds one repo that is checked out to a specific ID
type RefRepoCloningCheckout struct {
	repo *repo.Repository
	id   string
	pool *repoPool
}

func (c *RefRepoCloningCheckout) Path() string {
	return c.repo.Dir()
}

func (c *RefRepoCloningCheckout) ID() string {
	return c.id
}

func (c *RefRepoCloningCheckout) Release() error {
	c.pool.Release(c.repo)
	return nil
}
