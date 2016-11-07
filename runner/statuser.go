package runner

import (
	"math"
	"time"
)

// StateMask describes a set of States as a bitmask.
type StateMask uint64

const (
	UNKNOWN_MASK    StateMask = StateMask(1 << uint(UNKNOWN))
	PENDING_MASK              = 1 << uint(PENDING)
	PREPARING_MASK            = 1 << uint(PREPARING)
	RUNNING_MASK              = 1 << uint(RUNNING)
	COMPLETE_MASK             = 1 << uint(COMPLETE)
	FAILED_MASK               = 1 << uint(FAILED)
	ABORTED_MASK              = 1 << uint(ABORTED)
	TIMEDOUT_MASK             = 1 << uint(TIMEDOUT)
	BADREQUEST_MASK           = 1 << uint(BADREQUEST)
	DONE_MASK                 = (1<<uint(COMPLETE) |
		1<<uint(FAILED) |
		1<<uint(ABORTED) |
		1<<uint(TIMEDOUT))
	ALL_MASK = math.MaxUint64
)

// StatusQuery describes a query for Statuses.
// The Runs and States are and'ed: a ProcessStatus matches a StatusQuery
// if its ID is in q.Runs (or q.AllRuns) and its state is in q.States
type StatusQuery struct {
	Runs    []RunId   // Runs to query for
	AllRuns bool      // Whether to match all runs
	States  StateMask // What States to match
}

// PollOpts describes options for our Poll.
// It differs from StatusQuery because StatusQuery describes what ProcessStatus'es to match, but
// PollOpts describes the mechanics of how to perform the query.
type PollOpts struct {
	// How long to wait for new Events
	// If 0, return immediately (even if no matching statuses)
	// If positive, wait up to that time
	// If negative, wait until there are results
	Timeout time.Duration

	// We might add:
	// maximum number of evens to return
	// MaxEvents int
}

// Statuser allows a client to read ProcessStatus'es
type Statuser interface {

	// StatusQuery queries the statuses for statuses that match q, waiting according to opts
	StatusQuery(q StatusQuery, opts PollOpts) ([]ProcessStatus, error)

	// StatusQuerySingle is a convenience function. Its semantics are the same as StatusQuery,
	// but returns the only status (or an error if not 1 result)
	StatusQuerySingle(q StatusQuery, opts PollOpts) (ProcessStatus, error)

	// Legacy Functions
	// Status checks the status of run.
	Status(run RunId) (ProcessStatus, error)

	// Current status of all runs, running and finished, excepting any Erase()'s runs.
	StatusAll() ([]ProcessStatus, error)

	// Prunes the run history so StatusAll() can return a reasonable number of runs.
	Erase(run RunId) error
}

// Helper Functions to create StatusQuery's

// Query for when id is done
func RunDone(id RunId) StatusQuery {
	return StatusQuery{Runs: []RunId{id}, States: DONE_MASK}
}

// Query for the current state of id
func RunCurrent(id RunId) StatusQuery {
	return StatusQuery{Runs: []RunId{id}, States: ALL_MASK}
}

// Query for when id hits state
func RunState(id RunId, state ProcessState) StatusQuery {
	return StatusQuery{Runs: []RunId{id}, States: MaskForState(state)}
}

// Helper Function to create StateMasks
func MaskForState(state ProcessState) StateMask {
	return 1 << uint(state)
}

// Helper Functions to create PollOpts's

// Return the current results
func Current() PollOpts {
	return PollOpts{Timeout: time.Duration(0)}
}

// Wait until a match
func Wait() PollOpts {
	return PollOpts{Timeout: time.Duration(-1)}
}

// Implementations of Query Objects

// Matches checks if state matches m
func (m StateMask) Matches(state ProcessState) bool {
	return MaskForState(state)&m != 0
}

// Matches checks if st matches q
func (q StatusQuery) Matches(st ProcessStatus) bool {
	if !q.States.Matches(st.State) {
		return false
	}

	if q.AllRuns {
		return true
	}

	for _, id := range q.Runs {
		if id == st.RunId {
			return true
		}
	}

	return false
}
