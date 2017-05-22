package runner

import (
	"math"
	"time"
)

// status_rw.go is how to read/write RunStatus'es

// StateMask describes a set of States as a bitmask.
type StateMask uint64

// Useful StateMask constants
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

// Helper Function to create StateMask that matches exactly state
func MaskForState(state RunState) StateMask {
	return 1 << uint(state)
}

// Query describes a query for RunStatuses.
// The Runs and States are and'ed: a RunStatus matches a Query if its ID is in q.Runs (or q.AllRuns)
// and its state is in q.States
type Query struct {
	Runs    []RunID   // Runs to query for
	AllRuns bool      // Whether to match all runs
	States  StateMask // What States to match
}

// Wait describes how to Wait.
// It differs from StatusQuery because StatusQuery describes what RunStatus'es to match, but
// Wait describes how to Wait
type Wait struct {
	// How long to wait for Statuses
	Timeout time.Duration
	AbortCh chan bool
	Started chan bool

	// We might add whether to return as soon as one status matches, or waiting until all
}

// StatusQuerier allows reading Status by Query'ing.
type StatusQuerier interface {
	// Query returns all RunStatus'es matching q, waiting as described by w
	Query(q Query, w Wait) ([]RunStatus, ServiceStatus, error)

	StatusQueryNower
}

// StatusQueryNower allows Query'ing Statuses but with no Waiting
// This is separate from StatusQuerier because talking to the Worker, e.g., we will be able to
// QueryNow easily, but because Thrift doesn't like long waits on RPCs, it can't do
// Query with a decent wait. We want our type system to help protect us from this blowing up
// at runtime, so the RPC client will implement StatusQueryNower.
// We will implement a PollingQueuer that wraps a StatusQueryNower and satisfies StatusQuerier.
type StatusQueryNower interface {
	// QueryNow returns all RunStatus'es matching q in their current state
	QueryNow(q Query) ([]RunStatus, ServiceStatus, error)
}

// LegacyStatusReader contains legacy methods to read Status'es.
// Prefer using the convenience methods above.
type LegacyStatusReader interface {
	// Status returns the current status of id from q.
	Status(run RunID) (RunStatus, ServiceStatus, error)

	// StatusAll returns the Current status of all runs
	StatusAll() ([]RunStatus, ServiceStatus, error)
}

// StatusReader includes both the preferred and the legacy api.
type StatusReader interface {
	StatusQuerier

	// TODO(dbentley): remove
	LegacyStatusReader
}

// StatusWriter allows writing Statuses
type StatusWriter interface {
	// NewRun creates a new RunID in state Preparing
	NewRun() (RunStatus, error)

	// Update overall service status.
	UpdateService(st ServiceStatus) error

	// Update writes a new status.
	Update(st RunStatus) error

	StatusEraser
}

// StatusEraser allows Erasing a Status
type StatusEraser interface {
	// Prunes the run history so StatusAll() can return a reasonable number of runs.
	Erase(run RunID) error
}

func (m StateMask) Matches(state RunState) bool {
	return MaskForState(state)&m != 0
}

// Matches checks if st matches q
func (q Query) Matches(st RunStatus) bool {
	if !q.States.Matches(st.State) {
		return false
	}

	if q.AllRuns {
		return true
	}

	for _, id := range q.Runs {
		if id == st.RunID {
			return true
		}
	}

	return false
}
