package gitfiler

import (
	"io/ioutil"
	"path"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

// Utilities for creating Checkouters.

// RepoIniter lets a client initialize a Repository to use as a Reference Repository.
type RepoIniter interface {
	// Init initializes the Repository to use as a reference repository.
	Init() (*repo.Repository, error)
}

// RepoIniter implementation
type ConstantIniter struct {
	Repo *repo.Repository
}

func (g *ConstantIniter) Init() (*repo.Repository, error) {
	return g.Repo, nil
}

// A Pool that will only have a single repo, populated by repoIniter, that serves until doneCh is closed
func NewSingleRepoPool(repoIniter RepoIniter, doneCh <-chan struct{}) *RepoPool {
	singlePool := NewRepoPool(nil, nil, doneCh, 1)
	go func() {
		r, err := repoIniter.Init()
		singlePool.Release(r, err)
	}()
	return singlePool
}

// A Checkouter that checks out from a single repo populated by a NewSingleRepoPool)
func NewSingleRepoCheckouter(repoIniter RepoIniter, doneCh <-chan struct{}) *Checkouter {
	pool := NewSingleRepoPool(repoIniter, doneCh)
	return NewCheckouter(pool)
}

// A Checkouter that creates a new repo with git clone --reference for each checkout
func NewRefRepoCloningCheckouter(refRepoIniter RepoIniter,
	clonesDir *temp.TempDir,
	doneCh <-chan struct{},
	maxClones int) *Checkouter {
	refPool := NewSingleRepoPool(refRepoIniter, doneCh)

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

	pool := NewRepoPool(cloner, clones, doneCh, maxClones)
	return NewCheckouter(pool)
}
