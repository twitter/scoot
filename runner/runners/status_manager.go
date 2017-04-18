package runners

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/scootdev/scoot/runner"
)

const UnknownRunIDMsg = "unknown run id %v"

// NewStatusManager creates a new empty StatusManager
func NewStatusManager() *StatusManager {
	return &StatusManager{runs: make(map[runner.RunID]runner.RunStatus)}
}

// StatusManager is a database of RunStatus'es. It allows clients to Write StatusManager, Query the
// current status, and listen for updates to status. It implements runner.RunStatus
type StatusManager struct {
	mu        sync.Mutex
	runs      map[runner.RunID]runner.RunStatus
	svcStatus runner.ServiceStatus
	nextRunID int64
	listeners []queryAndCh
}

type queryAndCh struct {
	q  runner.Query
	ch chan runner.RunStatus
}

// Writer interface

// NewRun creates a new RunID in state Preparing
func (s *StatusManager) NewRun() (runner.RunStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := runner.RunID(strconv.FormatInt(s.nextRunID, 10))
	s.nextRunID++

	st := runner.RunStatus{
		RunID: id,
		State: runner.PENDING,
	}
	s.runs[id] = st
	return st, nil
}

// Update the overall service status independent of run status.
func (s *StatusManager) UpdateService(svcStatus runner.ServiceStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.svcStatus = svcStatus
	return nil
}

// Update writes a new status for a run.
// It enforces several rules:
//   cannot change a status once it is Done
//   cannot erase Stdout/Stderr Refs
func (s *StatusManager) Update(newStatus runner.RunStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldStatus, ok := s.runs[newStatus.RunID]
	if ok && oldStatus.State.IsDone() {
		return nil
	}

	if newStatus.StdoutRef == "" {
		newStatus.StdoutRef = oldStatus.StdoutRef
	}
	if newStatus.StderrRef == "" {
		newStatus.StderrRef = oldStatus.StderrRef
	}

	log.Debugf("StatusManager is holding status:%+v", newStatus)
	s.runs[newStatus.RunID] = newStatus

	listeners := make([]queryAndCh, 0, len(s.listeners))
	for _, listener := range s.listeners {
		if listener.q.Matches(newStatus) {
			log.Debugf("StatusManager putting status %+v on listener channel\n", newStatus)
			listener.ch <- newStatus
			close(listener.ch)
		} else {
			listeners = append(listeners, listener)
		}
	}
	s.listeners = listeners
	return nil
}

// Reader interface (implements runner.StatusQuerier)

// Query returns all RunStatus'es matching q, waiting as described by w, plus the overall service status.
func (s *StatusManager) Query(q runner.Query, wait runner.Wait) ([]runner.RunStatus, runner.ServiceStatus, error) {
	current, future, err := s.queryAndListen(q, wait.Timeout != 0)
	if err != nil || len(current) > 0 || wait.Timeout == 0 {
		return current, s.svcStatus, err
	}

	var timeout <-chan time.Time
	if wait.Timeout > 0 {
		ticker := time.NewTicker(wait.Timeout)
		timeout = ticker.C
		defer ticker.Stop()
	}

	select {
	case st := <-future:
		return []runner.RunStatus{st}, s.svcStatus, nil
	case <-timeout:
		return nil, s.svcStatus, nil
	}
}

// QueryNow returns all RunStatus'es matching q in their current state
func (s *StatusManager) QueryNow(q runner.Query) ([]runner.RunStatus, runner.ServiceStatus, error) {
	return s.Query(q, runner.Wait{})
}

// Status returns the current status of id from q.
func (s *StatusManager) Status(id runner.RunID) (runner.RunStatus, runner.ServiceStatus, error) {
	return runner.StatusNow(s, id)
}

// StatusAll returns the Current status of all runs
func (s *StatusManager) StatusAll() ([]runner.RunStatus, runner.ServiceStatus, error) {
	return runner.StatusAll(s)
}

// Prunes the run history so StatusAll() can return a reasonable number of runs.
func (s *StatusManager) Erase(run runner.RunID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.runs[run]
	if st.State.IsDone() {
		delete(s.runs, run)
	}
	return nil
}

// queryAndListen performs a query, returning the current results and optionally listens to the query
// returns:
//   the current results
//   a channel that will hold the next result (if current is empty and err is nil)
//   error
func (s *StatusManager) queryAndListen(q runner.Query, listen bool) (
	current []runner.RunStatus, future chan runner.RunStatus, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if q.AllRuns {
		for _, st := range s.runs {
			if q.States.Matches(st.State) {
				current = append(current, st)
			}
		}
	} else {
		for _, runID := range q.Runs {
			st, ok := s.runs[runID]
			if !ok {
				return nil, nil, fmt.Errorf(UnknownRunIDMsg, runID)
			}
			if q.States.Matches(st.State) {
				current = append(current, st)
			}
		}
	}

	if len(current) > 0 || !listen {
		return current, nil, err
	}

	ch := make(chan runner.RunStatus, 1)
	s.listeners = append(s.listeners, queryAndCh{q: q, ch: ch})
	return nil, ch, nil
}
