package gitfiler

import (
	"io/ioutil"
	"path"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

// Utilities for creating Checkouters.

// RepoGetter lets a client get a Repository to use as a Reference Repository.
type RepoGetter interface {
	// Get gets the Repository to use as a reference repository.
	Get() (*repo.Repository, error)
}

// RepoGetter implementation
type ConstantGetter struct {
	Repo *repo.Repository
}

func (g *ConstantGetter) Get() (*repo.Repository, error) {
	return g.Repo, nil
}

// A Pool that will only have a single repo, populated by repoGetter, that serves until doneCh is closed
func NewSingleRepoPool(repoGetter RepoGetter, doneCh <-chan struct{}) *RepoPool {
	singlePool := NewRepoPool(nil, nil, doneCh)
	go func() {
		r, err := repoGetter.Get()
		singlePool.Release(r, err)
	}()
	return singlePool
}

// A Checkouter that checks out from a single repo populated by a NewSingleRepoPool)
func NewSingleRepoCheckouter(repoGetter RepoGetter, doneCh <-chan struct{}) *Checkouter {
	pool := NewSingleRepoPool(repoGetter, doneCh)
	return NewCheckouter(pool)
}

// A Checkouter that creates a new repo with git clone --reference for each checkout
func NewRefRepoCloningCheckouter(refRepoGetter RepoGetter, clonesDir *temp.TempDir, doneCh <-chan struct{}) *Checkouter {
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

	// TODO why did we make 2 pools here
	pool := NewRepoPool(cloner, clones, doneCh)
	return NewCheckouter(pool)
}
