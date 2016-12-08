package runners

import (
	"time"

	"github.com/scootdev/scoot/runner"
)

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
