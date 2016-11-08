package runner

import (
	"time"
)

// StateMask describes a set of States as a bitmask.
type StateMask uint64

// StatusQuery describes a query for Statuses.
// The Runs and States are and'ed: a ProcessStatus matches a StatusQuery
// if its ID is in q.Runs (or q.AllRuns) and its state is in q.States
type StatusQuery struct {
	Runs    []RunId   // Runs to query for
	AllRuns bool      // Whether to match all runs
	States  StateMask // What States to match
}

// StatusesSync allows Querying the status of Processes synchronously
type StatusesSync interface {
	// QuerySync queries the current statuses for statuses that match q.
	// Returns the ProcessStatus'es that match the Query.
	// (i.e., if the query is for 3 RunIds but only one matches the StateMask,
	// only one will be returned)
	QuerySync(q StatusQuery) ([]ProcessStatus, error)
}

// Statuses allows Querying the status of Processes
type Statuses interface {
	// QueryWait queries the statuses for statuses that match q, waiting if none match now.
	// Returns the ProcessStatus'es that match the Query.
	// (i.e., if the query is for 3 RunIds but only one matches the StateMask,
	// only one will be returned)
	// If timeout is 0, same as QuerySync
	// If positive, wait up to that long
	Query(q StatusQuery, timeout time.Duration) ([]ProcessStatus, error)

	StatusesSync
}

// StatusWriter allows updating Statuses
type StatusWriter interface {
	// NewRun creates a new run, with a new RunId in status PENDING
	NewRun() (ProcessStatus, error)

	// Update writes a new status.
	// It enforces several rules (not errors)
	//   cannot change a status once it is Done
	//   cannot erase Stdout/Stderr Refs (but can update to new values)
	Update(ProcessStatus) error
}

// LegacyStatuses is the old Read API. Statuses can do everything it can do (and more),
// but we don't want to break current code. Depreate. Prefer Statuses.
type LegacyStatuses interface {

	// Status checks the status of run.
	Status(run RunId) (ProcessStatus, error)

	// Current status of all runs, running and finished, excepting any Erase()'s runs.
	StatusAll() ([]ProcessStatus, error)

	// Prunes the run history so StatusAll() can return a reasonable number of runs.
	Erase(run RunId) error
}
