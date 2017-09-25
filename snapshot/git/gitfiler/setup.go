package gitfiler

import (
	"io/ioutil"
	"path"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/snapshot/git/repo"
)

// Utilities for creating Checkouters.

// PooledRepoIniter is an interface for initializing repositories
// Not widely used, renamed from RepoIniter for separation from gitdb.RepoIniter
type PooledRepoIniter interface {
	// Init initializes the Repository to use as a reference repository
	// Takes a StatsReceiver to support instrumenting repo initialization
	Init(stat stats.StatsReceiver) (*repo.Repository, error)
}

// PooledRepoIniter implementation
type ConstantIniter struct {
	Repo *repo.Repository
}

func (g *ConstantIniter) Init(stat stats.StatsReceiver) (*repo.Repository, error) {
	return g.Repo, nil
}

// Util for creating a Pool that will only have a single repo
func NewSingleRepoPool(repoIniter PooledRepoIniter,
	stat stats.StatsReceiver,
	doneCh <-chan struct{}) *RepoPool {
	singlePool := NewRepoPool(repoIniter, stat, []*repo.Repository{}, doneCh, 1)
	return singlePool
}

// A Checkouter that checks out from a single repo populated by a NewSingleRepoPool)
func NewSingleRepoCheckouter(repoIniter PooledRepoIniter,
	stat stats.StatsReceiver,
	doneCh <-chan struct{}) *Checkouter {
	pool := NewSingleRepoPool(repoIniter, stat, doneCh)
	return NewCheckouter(pool, stat)
}

// A Checkouter that creates a new repo with git clone --reference for each checkout
func NewRefRepoCloningCheckouter(refRepoIniter PooledRepoIniter,
	stat stats.StatsReceiver,
	clonesDir *temp.TempDir,
	doneCh <-chan struct{},
	maxClones int) *Checkouter {
	refPool := NewSingleRepoPool(refRepoIniter, stat, doneCh)

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

	pool := NewRepoPool(cloner, stat, clones, doneCh, maxClones)
	return NewCheckouter(pool, stat)
}
