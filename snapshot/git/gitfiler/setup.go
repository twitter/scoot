package gitfiler

import (
	"io/ioutil"
	"log"
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
	// TODO create a pool with nothing
	//	then manually create a repo via the getter with Get()
	//	'Release' the repo into the pool async (this func already returned)
	singlePool := NewRepoPool(nil, nil, doneCh, 1)
	go func() {
		log.Println("NewSingleRepoPool's gofunc repoGetter.Get()")
		r, err := repoGetter.Get()
		log.Println("NewSingleRepoPool's gofunc singlePool.Release()")
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
// sci workerserver main entry point (w/ a constantgetter)
func NewRefRepoCloningCheckouter(refRepoGetter RepoGetter, clonesDir *temp.TempDir, doneCh <-chan struct{}) *Checkouter {
	log.Println("New single repo pool")
	refPool := NewSingleRepoPool(refRepoGetter, doneCh)

	cloner := &refCloner{refPool: refPool, clonesDir: clonesDir}
	var clones []*repo.Repository
	fis, _ := ioutil.ReadDir(clonesDir.Dir)
	for _, fi := range fis {
		// TODO(dbentley): maybe we should check that these clones are in fact clones
		// of the reference repo? Using some kind of git commands to determine its upstream?
		if clone, err := repo.NewRepository(path.Join(clonesDir.Dir, fi.Name())); err == nil {
			log.Println("Append clone", fi.Name(), "to repo slice clones")
			clones = append(clones, clone)
		}
	}

	log.Println("Another new repo pool, w/ cloners")
	pool := NewRepoPool(cloner, clones, doneCh, 1)
	return NewCheckouter(pool)
}
