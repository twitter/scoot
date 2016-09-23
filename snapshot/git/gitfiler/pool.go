package gitfiler

import (
	"github.com/scootdev/scoot/snapshot/git/repo"
)

type repoAndError struct {
	repo *repo.Repository
	err  error
}

func newRepoPool(cloner *cloner, doneCh chan struct{}) *repoPool {
	p := &repoPool{
		cloner:    cloner,
		releaseCh: make(chan *repo.Repository),
		reserveCh: make(chan repoAndError),
		doneCh:    doneCh,
	}
	go p.loop()
	return p
}

// repoPool holds repos ready to be used.
// If none are ready, it will clone a new one in the background using cloner
type repoPool struct {
	cloner *cloner

	releaseCh chan *repo.Repository
	reserveCh chan repoAndError
	cloneCh   chan repoAndError
	doneCh    chan struct{}

	freeList []repoAndError
}

// Release releases a clone that is no longer needed
func (p *repoPool) Release(clone *repo.Repository) {
	p.releaseCh <- clone
}

// Reserve reserves a clone, or returns an error if it cannot be cloned
func (p *repoPool) Reserve() (*repo.Repository, error) {
	r := <-p.reserveCh
	return r.repo, r.err
}

func (p *repoPool) loop() {
	for {
		// If we don't have any free repos, and we're not currently cloning, start a clone
		if len(p.freeList) == 0 && p.cloneCh == nil {
			p.cloneCh = make(chan repoAndError)
			go func() {
				r, err := p.cloner.clone()
				p.cloneCh <- repoAndError{r, err}
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
			p.freeList = append(p.freeList, repoAndError{repo: r})
		case r := <-p.cloneCh:
			p.freeList = append(p.freeList, r)
			p.cloneCh = nil
		case <-p.doneCh:
			return
		}

	}
}
