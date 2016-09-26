package gitfiler

import (
	"github.com/scootdev/scoot/snapshot/git/repo"
)

type repoAndError struct {
	repo *repo.Repository
	err  error
}

func NewRepoPool(getter RepoGetter, repos []*repo.Repository, doneCh chan struct{}) *RepoPool {
	freeList := make([]repoAndError, len(repos))
	for i, v := range repos {
		freeList[i] = repoAndError{repo: v}
	}

	p := &RepoPool{
		getter:    getter,
		releaseCh: make(chan repoAndError, 1),
		reserveCh: make(chan repoAndError),
		doneCh:    doneCh,
		freeList:  freeList,
	}
	go p.loop()
	return p
}

// repoPool holds repos ready to be used.
// If none are ready, it will clone a new one in the background using cloner
type RepoPool struct {
	getter RepoGetter

	releaseCh chan repoAndError
	reserveCh chan repoAndError
	doneCh    chan struct{}

	freeList []repoAndError
}

// Release releases a clone that is no longer needed
func (p *RepoPool) Release(repo *repo.Repository, err error) {
	p.releaseCh <- repoAndError{repo, err}
}

// Reserve reserves a clone, or returns an error if it cannot be cloned
func (p *RepoPool) Get() (*repo.Repository, error) {
	r := <-p.reserveCh
	return r.repo, r.err
}

func (p *RepoPool) loop() {
	for {
		if len(p.freeList) == 0 && p.getter != nil {
			go func() {
				r, err := p.getter.Get()
				p.Release(r, err)
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
		case <-p.doneCh:
			return
		}

	}
}
