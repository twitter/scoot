package runners

import (
	"github.com/scootdev/scoot/runner"
)

type StatusesRO []runner.RunStatus

func (s StatusesRO) QueryNow(q runner.Query) ([]runner.RunStatus, error) {
	var r []runner.RunStatus
	for _, st := range s {
		if q.Matches(st) {
			r = append(r, st)
		}
	}
	return r, nil
}
