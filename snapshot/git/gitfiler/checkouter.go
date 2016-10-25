package gitfiler

import (
	"fmt"

	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

func NewCheckouter(repos *RepoPool) *Checkouter {
	return &Checkouter{repos: repos}
}

// Checkouter checks out by checking out in a repo from pool
type Checkouter struct {
	repos *RepoPool
}

// An arbitrary revision. As mentioned below, we should get rid of this altogether
const DEFAULT_REV = "1dda9fbde682e4922a0d5709c5539f573db4cc54"

// Checkout checks out id (a raw git sha) into a Checkout.
// It does this by making a new clone (via reference) and checking out id.
// TODO(dbentley): if id is not found because it is present in upstream repos but not here,
// we should fetch it and check it out.
func (c *Checkouter) Checkout(id string) (co snapshot.Checkout, err error) {
	repo, repoErr := c.repos.Get()
	if repoErr != nil {
		return nil, repoErr
	}

	// release if we aren't using it
	defer func() {
		if err != nil || recover() != nil {
			c.repos.Release(repo, repoErr)
		}
	}()

	// TODO(dbentley): do more ot validate id. E.g., don't let "HEAD" or "master" slip through
	if id == "" {
		id = DEFAULT_REV
	}

	// -d removes directories. -x ignores gitignore and removes everything.
	// -f is force. -f the second time removes directories even if they're git repos themselves
	cmds := [][]string{
		{"clean", "-f", "-f", "-d", "-x"},
		{"checkout", id},
	}

	for _, argv := range cmds {
		if _, err := repo.Run(argv...); err != nil {
			return nil, fmt.Errorf("gitfiler.Checkouter.Checkout: %v", err)
		}
	}
	return &Checkout{repo: repo, id: id, pool: c.repos}, nil
}

// Checkout holds one repo that is checked out to a specific ID
type Checkout struct {
	repo *repo.Repository
	id   string
	pool *RepoPool
}

func (c *Checkout) Path() string {
	return c.repo.Dir()
}

func (c *Checkout) ID() string {
	return c.id
}

func (c *Checkout) Release() error {
	c.pool.Release(c.repo, nil)
	return nil
}

// TODO placeholder
type ConstantGetter struct {
	Repo *repo.Repository
}

func (g *ConstantGetter) Get() (*repo.Repository, error) {
	return g.Repo, nil
}
