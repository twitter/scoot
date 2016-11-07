package runner

import (
	"math"
	"time"
)

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

type StatusQuery struct {
	Runs    []RunId
	AllRuns bool
	States  StateMask
}

// PollOpts describes options for our Poll.
// It differs from StatusQuery because StatusQuery describes what ProcessStatus'es to match, but
// PollOpts describes the mechanics of how to perform the query.
type PollOpts struct {
	// How long to wait for new Events
	Timeout time.Duration

	// We might add:
	// maximum number of evens to return
	// MaxEvents int
}

type Statuser interface {
	StatusQuery(q StatusQuery, opts PollOpts) ([]ProcessStatus, error)

	// Convenience Function
	StatusQuerySingle(q StatusQuery, opts PollOpts) (ProcessStatus, error)

	// Legacy
	Status(run RunId) (ProcessStatus, error)
	StatusAll() ([]ProcessStatus, error)

	// Prunes the run history so StatusAll() can return a reasonable number of runs.
	Erase(run RunId) error
}

// Helper Functions to create StatusQuery's

func RunDone(id RunId) StatusQuery {
	return StatusQuery{Runs: []RunId{id}, States: DONE_MASK}
}

func RunCurrent(id RunId) StatusQuery {
	return StatusQuery{Runs: []RunId{id}, States: ALL_MASK}
}

func RunState(id RunId, state ProcessState) StatusQuery {
	return StatusQuery{Runs: []RunId{id}, States: MaskForState(state)}
}

// Helper Function to create StateMasks
func MaskForState(state ProcessState) StateMask {
	return 1 << uint(state)
}

// Helper Functions to create PollOpts's
func Current() PollOpts {
	return PollOpts{Timeout: time.Duration(0)}
}

func Wait() PollOpts {
	return PollOpts{Timeout: time.Duration(-1)}
}

// Implementations of Query Objects
func (m StateMask) Matches(state ProcessStatus) bool {
	return MaskForState(state)&m != 0
}

func (q StatusQuery) Matches(st ProcessStatus) bool {
	if !q.States.Matches(st) {
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
