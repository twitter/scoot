package runners

import (
	"fmt"
	"sync"

	"github.com/scootdev/scoot/runner"
)

const UnknownRunIDMsg = "unknown run id %v"

// NewStatuses creates a new empty Statuses
func NewStatuses() *Statuses {
	return &Statuses{runs: make(map[runner.RunID]runner.RunStatus)}
}

// Statuses is a database of RunStatus'es. It allows clients to Write Statuses, Query the
// current status, and listen for updates to status. It implements runner.RunStatus
type Statuses struct {
	mu        sync.Mutex
	runs      map[runner.RunID]runner.RunStatus
	nextRunID int64
}

// Writer interface

// NewRun creates a new RunID in state Preparing
func (s *Statuses) NewRun() (runner.RunStatus, error) {
	return runner.RunStatus{}, fmt.Errorf("not yet implemented")
}

// Update writes a new status.
// It enforces several rules:
//   cannot change a status once it is Done
//   cannot erase Stdout/Stderr Refs
func (s *Statuses) Update(newStatus runner.RunStatus) error {
	return fmt.Errorf("not yet implemented")
}

// Reader interface (implements runner.StatusQuerier)

// Query returns all RunStatus'es matching q, waiting as described by w
func (s *Statuses) Query(q runner.Query, wait runner.Wait) ([]runner.RunStatus, error) {
	return nil, fmt.Errorf("not yet implemented")
}

// QueryNow returns all RunStatus'es matching q in their current state
func (s *Statuses) QueryNow(q runner.Query) ([]runner.RunStatus, error) {
	return nil, fmt.Errorf("not yet implemented")
}

// Status returns the current status of id from q.
func (s *Statuses) Status(id runner.RunID) (runner.RunStatus, error) {
	return runner.Status(s, id)
}

// StatusAll returns the Current status of all runs
func (s *Statuses) StatusAll() ([]runner.RunStatus, error) {
	return runner.StatusAll(s)
}

// Prunes the run history so StatusAll() can return a reasonable number of runs.
func (s *Statuses) Erase(run runner.RunID) error {
	return fmt.Errorf("not yet implemented")
}
