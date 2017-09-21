package runners

import (
	"errors"
	"time"

	"github.com/twitter/scoot/runner"
)

// polling.go: turns a StatusQueryNower into a StatusQuerier by polling

// NewPollingStatusQuerier creates a new StatusQuerier by polling a StatusQueryNower that polls every period
func NewPollingStatusQuerier(del runner.StatusQueryNower, period time.Duration) *PollingStatusQuerier {
	runner := &PollingStatusQuerier{del, period}
	return runner
}

// NewPollingService creates a new Service from a Controller, a StatusEraser, and a StatusQueryNower.
// (This is a convenience function over NewPollingStatusQuerier
func NewPollingService(c runner.Controller, e runner.StatusEraser, nower runner.StatusQueryNower, period time.Duration) runner.Service {
	q := NewPollingStatusQuerier(nower, period)
	return &Service{c, q, e}
}

// PollingStatusQuerier turns a StatusQueryNower into a StatusQuerier
type PollingStatusQuerier struct {
	del    runner.StatusQueryNower
	period time.Duration
}

// QueryNow returns all RunStatus'es matching q in their current state
func (r *PollingStatusQuerier) QueryNow(q runner.Query) ([]runner.RunStatus, runner.ServiceStatus, error) {
	return r.del.QueryNow(q)
}

// Query returns all RunStatus'es matching q, waiting as described by w
func (r *PollingStatusQuerier) Query(q runner.Query, wait runner.Wait) ([]runner.RunStatus, runner.ServiceStatus, error) {
	end := time.Now().Add(wait.Timeout)
	var service runner.ServiceStatus
	for time.Now().Before(end) || wait.Timeout == 0 {
		select {
		case <-wait.AbortCh:
			return nil, service, errors.New("Aborted")
		default:
		}
		st, service, err := r.QueryNow(q)
		if err != nil || len(st) > 0 {
			return st, service, err
		}
		time.Sleep(r.period)
	}
	return nil, service, nil
}

// Status returns the current status of id from q.
func (r *PollingStatusQuerier) Status(id runner.RunID) (runner.RunStatus, runner.ServiceStatus, error) {
	return runner.StatusNow(r, id)
}

// StatusAll returns the Current status of all runs
func (r *PollingStatusQuerier) StatusAll() ([]runner.RunStatus, runner.ServiceStatus, error) {
	return runner.StatusAll(r)
}
