package runners

import (
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/scootdev/scoot/runner"
	"log"
)

// polling.go: turns a StatusQueryNower into a StatusQuerier by polling

// NewPollingStatusQuerier creates a new StatusQuerier by polling a StatusQueryNower that polls every period
func NewPollingStatusQuerier(del runner.StatusQueryNower, period time.Duration) *PollingStatusQuerier {
	runner := &PollingStatusQuerier{del, period}
	log.Println("************** runner definition")
	runnerDesc := spew.Sdump(runner)
	log.Println(runnerDesc)
	return runner
}

// NewPollingService creates a new Service from a Controller, a StatusEraser, and a StatusQueryNower.
// (This is a convenience function over NewPollingStatusQuerier
func NewPollingService(c runner.Controller, e runner.StatusEraser, nower runner.StatusQueryNower, period time.Duration) runner.Service {
	q := NewPollingStatusQuerier(nower, period)
	runner := &Service{c, q, e}
	log.Println("************** runner definition")
	runnerDesc := spew.Sdump(runner)
	log.Println(runnerDesc)
	return runner
}

// PollingStatusQuerier turns a StatusQueryNower into a StatusQuerier
type PollingStatusQuerier struct {
	del    runner.StatusQueryNower
	period time.Duration
}

// QueryNow returns all RunStatus'es matching q in their current state
func (r *PollingStatusQuerier) QueryNow(q runner.Query) ([]runner.RunStatus, error) {
	return r.del.QueryNow(q)
}

// Query returns all RunStatus'es matching q, waiting as described by w
func (r *PollingStatusQuerier) Query(q runner.Query, wait runner.Wait) ([]runner.RunStatus, error) {
	end := time.Now().Add(wait.Timeout)
	for time.Now().Before(end) || wait.Timeout == 0 {
		st, err := r.QueryNow(q)
		if err != nil || len(st) > 0 {
			return st, err
		}
		time.Sleep(r.period)
	}
	return nil, nil
}

// Status returns the current status of id from q.
func (r *PollingStatusQuerier) Status(id runner.RunID) (runner.RunStatus, error) {
	return runner.StatusNow(r, id)
}

// StatusAll returns the Current status of all runs
func (r *PollingStatusQuerier) StatusAll() ([]runner.RunStatus, error) {
	return runner.StatusAll(r)
}
