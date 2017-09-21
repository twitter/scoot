package runners

import (
	"github.com/twitter/scoot/runner"
)

// StatusesRO is an adapter that lets you query a slice of RunStatus
type StatusesRO []runner.RunStatus

// QueryNow satisfies runner.QueryNower
func (s StatusesRO) QueryNow(q runner.Query) ([]runner.RunStatus, error) {
	var r []runner.RunStatus
	for _, st := range s {
		if q.Matches(st) {
			r = append(r, st)
		}
	}
	return r, nil
}
