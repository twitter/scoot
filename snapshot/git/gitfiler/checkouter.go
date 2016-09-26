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

// RepoGetter lets a client get a Repository to use as a Reference Repository.
type RepoGetter interface {
	// Get gets the Repository to use as a reference repository.
	Get() (*repo.Repository, error)
}

func NewSingleRepoPool(repoGetter RepoGetter, doneCh chan struct{}) *RepoPool {
	singlePool := NewRepoPool(nil, nil, doneCh)
	go func() {
		r, err := repoGetter.Get()
		singlePool.Release(r, err)
	}()
	return singlePool
}

func NewSingleRepoCheckouter(repoGetter RepoGetter, doneCh chan struct{}) *Checkouter {
	pool := NewSingleRepoPool(repoGetter, doneCh)
	return NewCheckouter(pool)
}

func NewRefRepoCloningCheckouter(refRepoGetter RepoGetter, clonesDir *temp.TempDir, doneCh chan struct{}) *Checkouter {
	refPool := NewSingleRepoPool(refRepoGetter, doneCh)

	cloner := &refCloner{refPool: refPool, clonesDir: clonesDir}
	var clones []*repo.Repository
	fis, _ := ioutil.ReadDir(clonesDir.Dir)
	for _, fi := range fis {
		// TODO(dbentley): maybe we should check that these clones are in fact clones
		// of the reference repo? Using some kind of git commands to determine its upstream?
		if clone, err := repo.NewRepository(path.Join(clonesDir.Dir, fi.Name())); err == nil {
			clones = append(clones, clone)
		}
	}

	pool := NewRepoPool(cloner, clones, doneCh)
	return NewCheckouter(pool)
}

func NewCheckouter(repos *RepoPool) *Checkouter {
	return &Checkouter{repos: repos}
}

// Checkouter checks out by checking out in a repo from pool
type Checkouter struct {
	repos *RepoPool
}

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
