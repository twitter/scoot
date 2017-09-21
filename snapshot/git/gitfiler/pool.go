package gitfiler

import (
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/snapshot/git/repo"
)

// repoAndError holds a repo and an error (just so we can pass it across a channel)
type repoAndError struct {
	repo *repo.Repository
	err  error
}

// NewRepoPool creates a new RepoPool populated with existing repos and a initer that can get new ones
func NewRepoPool(initer PooledRepoIniter,
	stat stats.StatsReceiver,
	repos []*repo.Repository,
	doneCh <-chan struct{},
	capacity int) *RepoPool {

	freeList := make([]repoAndError, len(repos))
	for i, v := range repos {
		freeList[i] = repoAndError{repo: v}
	}

	p := &RepoPool{
		initer:    initer,
		stat:      stat,
		releaseCh: make(chan repoAndError),
		reserveCh: make(chan repoAndError),
		doneCh:    doneCh,
		freeList:  freeList,
		numInited: len(repos),
		capacity:  capacity,
	}
	go p.loop()
	return p
}

// RepoPool lets clients Get a Repository for an operation, and then Release it when done
// RepoPool can create a new Repository by using a supplied PooledRepoIniter.
// New repos can be added by the client by Release'ing a new Repository
// Cf. sync's Pool
// Supports maximum pool capacity - 0 is unlimited
type RepoPool struct {
	initer PooledRepoIniter
	stat   stats.StatsReceiver

	releaseCh chan repoAndError
	reserveCh chan repoAndError
	initCh    chan repoAndError
	doneCh    <-chan struct{}

	freeList  []repoAndError
	numInited int
	capacity  int
}

// Get gets a repo, or returns an error if it can't be gotten
func (p *RepoPool) Get() (*repo.Repository, error) {
	r := <-p.reserveCh
	return r.repo, r.err
}

// Release releases a repo that is no longer needed
func (p *RepoPool) Release(repo *repo.Repository, err error) {
	p.releaseCh <- repoAndError{repo, err}
}

func (p *RepoPool) loop() {
	for {
		// kick off a get if: empty, have room, have initer, not initializing
		if len(p.freeList) == 0 &&
			(p.capacity == 0 || p.numInited < p.capacity) &&
			p.initer != nil &&
			p.initCh == nil {
			// buffer of 1 to unblock background get if we're done before it finishes
			p.initCh = make(chan repoAndError, 1)
			go func() {
				r, err := p.initer.Init(p.stat)
				p.initCh <- repoAndError{r, err}
			}()
		}

		var reserveCh chan repoAndError
		var free repoAndError
		if len(p.freeList) > 0 {
			reserveCh, free = p.reserveCh, p.freeList[0]
		}

		// Serve, either receiving or sending repos
		select {
		case reserveCh <- free:
			p.freeList = p.freeList[1:]
		case r := <-p.releaseCh:
			p.freeList = append(p.freeList, r)
		case r := <-p.initCh:
			p.freeList = append(p.freeList, r)
			p.numInited++
			p.initCh = nil
		case <-p.doneCh:
			return
		}

	}
}
