package gitfiler

import (
	"log"

	"github.com/scootdev/scoot/snapshot/git/repo"
)

// repoAndError holds a repo and an error (just so we can pass it across a channel)
type repoAndError struct {
	repo *repo.Repository
	err  error
}

// NewRepoPool creates a new RepoPool populated with existing repos and a getter that can get new ones
func NewRepoPool(getter RepoGetter,
	repos []*repo.Repository,
	doneCh <-chan struct{},
	capacity int) *RepoPool {

	freeList := make([]repoAndError, len(repos))
	for i, v := range repos {
		freeList[i] = repoAndError{repo: v}
	}

	p := &RepoPool{
		getter:    getter,
		releaseCh: make(chan repoAndError),
		reserveCh: make(chan repoAndError),
		doneCh:    doneCh,
		freeList:  freeList,
		size:      0,
		capacity:  capacity,
	}
	go p.loop()
	return p
}

// RepoPool lets clients Get a Repository for an operation, and then Release it when done
// RepoPool can create a new Repository by using a supplied RepoGetter.
// New repos can be added by the client by Release'ing a new Repository
// Cf. sync's Pool
type RepoPool struct {
	getter RepoGetter

	releaseCh chan repoAndError
	reserveCh chan repoAndError
	getCh     chan repoAndError
	doneCh    <-chan struct{}

	freeList []repoAndError
	size     int
	capacity int
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
		// kick off a get if: empty, have room, have non-nil getter, aren't getting
		if len(p.freeList) == 0 &&
			p.size < p.capacity &&
			p.getter != nil &&
			p.getCh == nil {
			// buffer of 1 to unblock background get if we're done before it finishes
			p.getCh = make(chan repoAndError, 1)
			go func() {
				log.Println("RepoPool's gofunc - getter.Get")
				r, err := p.getter.Get()
				log.Println("RepoPool's gofunc - send status into getCh")
				p.getCh <- repoAndError{r, err}
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
			log.Println("Repo reserved via reserveCh - freeList:", len(p.freeList))
		case r := <-p.releaseCh:
			p.freeList = append(p.freeList, r)
			log.Println("Repo released via releaseCh - freeList:", len(p.freeList))
		case r := <-p.getCh:
			p.freeList = append(p.freeList, r)
			p.size++
			p.getCh = nil
			log.Println("Repo added via getCh - freeList:", len(p.freeList))
		case <-p.doneCh:
			log.Println("RepoPool exiting via doneCh")
			return
		}

	}
}
