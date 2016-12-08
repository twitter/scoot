package runners

import (
	"time"

	"github.com/scootdev/scoot/runner"
)

func NewPollingStatusQuerier(del runner.StatusQueryNower, period time.Duration) *PollingStatusQuerier {
	return &PollingStatusQuerier{del, period}
}

func NewPollingService(c runner.Controller, e runner.StatusEraser, nower runner.StatusQueryNower, period time.Duration) runner.Service {
	q := NewPollingStatusQuerier(nower, period)
	return &ServiceFacade{c, q, e}
}

type PollingStatusQuerier struct {
	del    runner.StatusQueryNower
	period time.Duration
}

func (r *PollingStatusQuerier) QueryNow(q runner.Query) ([]runner.RunStatus, error) {
	return r.del.QueryNow(q)
}

func (r *PollingStatusQuerier) Query(q runner.Query, wait runner.Wait) ([]runner.RunStatus, error) {
	if wait.Timeout == 0 {
		return r.del.QueryNow(q)
	}
	end := time.Now().Add(wait.Timeout)
	for time.Now().Before(end) {
		st, err := r.del.QueryNow(q)
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
