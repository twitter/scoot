package runner

import (
	"fmt"
	"time"
)

// StateMask describes a set of States as a bitmask.
type StateMask uint64

// Query describes a query for RunStatuses.
// The Runs and States are and'ed: a RunStatus matches a Query
// if its ID is in q.Runs (or q.AllRuns) and its state is in q.States
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

	// We might add whether to return as soon as one status matches, or waiting until all
}

// StatusQuerier allows reading Status by Query'ing.
type StatusQuerier interface {
	// Query returns all RunStatus'es matching q, waiting as described by w
	Query(q Query, w Wait) ([]RunStatus, error)

	StatusQueryNower
}

// StatusQueryNower allows Query'ing Statuses but with no Waiting
type StatusQueryNower interface {
	// QueryNow returns all RunStatus'es matching q in their current state
	QueryNow(q Query) ([]RunStatus, error)
}

// Convenience methods for common queries in terms of the more general Query interface.

// Status returns the current status of id from q.
func Status(q StatusQueryNower, id RunID) (RunStatus, error) {
	return RunStatus{}, fmt.Errorf("Not yet implemented")
}

// StatusAll returns the Current status of all runs
func StatusAll(q StatusQueryNower) ([]RunStatus, error) {
	return nil, fmt.Errorf("not yet implemented")
}

// LegacyStatusReader contains legacy methods to read Status'es.
// Prefer using the convenience methods above.
type LegacyStatusReader interface {
	// Status returns the current status of id from q.
	Status(run RunID) (RunStatus, error)

	// StatusAll returns the Current status of all runs
	StatusAll() ([]RunStatus, error)
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

	// Update writes a new status.
	UpdateStatus(st RunStatus) error

	StatusEraser
}

// StatusErraser allows Erasing a Status
type StatusEraser interface {
	// Prunes the run history so StatusAll() can return a reasonable number of runs.
	Erase(run RunID) error
}
