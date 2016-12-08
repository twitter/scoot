package runner

import (
	"math"
	"time"
)

// Convenience methods for common queries in terms of the more general Query interface.

func WaitForever() Wait {
	return Wait{Timeout: time.Duration(math.MaxInt64)}
}

// Status returns the current status of id from q.
func StatusNow(q StatusQueryNower, id RunID) (RunStatus, error) {
	statuses, err := q.QueryNow(Query{Runs: []RunID{id}, States: ALL_MASK})
	if err != nil {
		return RunStatus{}, err
	}
	return statuses[0], nil
}

func FinalStatus(q StatusQuerier, id RunID) (RunStatus, error) {
	statuses, err := q.Query(Query{Runs: []RunID{id}, States: DONE_MASK}, WaitForever())
	return SingleStatus(statuses, err)
}

func WaitForState(q StatusQuerier, id RunID, expected RunState) (RunStatus, error) {
	statuses, err := q.Query(Query{Runs: []RunID{id}, States: MaskForState(expected)}, WaitForever())
	return SingleStatus(statuses, err)
}

func SingleStatus(statuses []RunStatus, err error) (RunStatus, error) {
	if err != nil {
		return RunStatus{}, err
	}
	return statuses[0], nil
}

// StatusAll returns the Current status of all runs
func StatusAll(q StatusQueryNower) ([]RunStatus, error) {
	return q.QueryNow(Query{States: ALL_MASK, AllRuns: true})
}
