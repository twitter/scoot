package runners

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/scootdev/scoot/runner"
)

const UnknownRunIdMsg = "unknown run id %v"

// NewStatuses creates a new empty Statuses
func NewStatuses() *Statuses {
	return &Statuses{runs: make(map[runner.RunId]runner.ProcessStatus)}
}

// Statuses is a database of ProcessStatus'es. It allows clients to Write Statuses, Query the
// current status, and listen for updates to status. It implements runner.ProcessStatus
type Statuses struct {
	mu        sync.Mutex
	runs      map[runner.RunId]runner.ProcessStatus
	nextRunId int64
	listeners []queryAndCh
}

type queryAndCh struct {
	q  runner.StatusQuery
	ch chan runner.ProcessStatus
}

// Writer interface

// NewRun creates a new run, with a new RunId in status Preparing
func (s *Statuses) NewRun() runner.ProcessStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := runner.RunId(strconv.FormatInt(s.nextRunId, 10))
	s.nextRunId++

	st := runner.ProcessStatus{
		RunId: id,
		State: runner.PENDING,
	}
	s.runs[id] = st
	return st
}

// Update writes a new status.
// It enforces several rules:
//   cannot change a status once it is Done
//   cannot erase Stdout/Stderr Refs
func (s *Statuses) Update(newStatus runner.ProcessStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldStatus, ok := s.runs[newStatus.RunId]
	if ok && oldStatus.State.IsDone() {
		return
	}

	if newStatus.StdoutRef == "" {
		newStatus.StdoutRef = oldStatus.StdoutRef
	}
	if newStatus.StderrRef == "" {
		newStatus.StderrRef = oldStatus.StderrRef
	}

	s.runs[newStatus.RunId] = newStatus

	listeners := make([]queryAndCh, 0, len(s.listeners))
	for _, listener := range s.listeners {
		if listener.q.Matches(newStatus) {
			listener.ch <- newStatus
			close(listener.ch)
		} else {
			listeners = append(listeners, listener)
		}
	}
	s.listeners = listeners
}

// Reader interface (implements runner.Statuser)

func (s *Statuses) StatusQuery(q runner.StatusQuery, poll runner.PollOpts) ([]runner.ProcessStatus, error) {
	current, future, err := s.queryAndListen(q, poll.Timeout != 0)
	if err != nil || len(current) > 0 || poll.Timeout == 0 {
		return current, err
	}

	var timeout <-chan time.Time
	if poll.Timeout > 0 {
		ticker := time.NewTicker(poll.Timeout)
		timeout = ticker.C
		defer ticker.Stop()
	}

	select {
	case st := <-future:
		return []runner.ProcessStatus{st}, nil
	case <-timeout:
		return nil, nil
	}
}

func (s *Statuses) StatusQuerySingle(q runner.StatusQuery, poll runner.PollOpts) (runner.ProcessStatus, error) {
	statuses, err := s.StatusQuery(q, poll)
	if err != nil {
		return runner.ProcessStatus{}, err
	}

	if len(statuses) != 1 {
		return runner.ProcessStatus{}, fmt.Errorf("StatusQuerySingle expected 1 result; got %d: %v", len(statuses), statuses)
	}

	return statuses[0], nil
}

func (s *Statuses) Status(id runner.RunId) (runner.ProcessStatus, error) {
	return s.StatusQuerySingle(runner.RunCurrent(id), runner.Current())
}

func (s *Statuses) StatusAll() ([]runner.ProcessStatus, error) {
	return s.StatusQuery(runner.StatusQuery{AllRuns: true, States: runner.ALL_MASK}, runner.Current())
}

func (s *Statuses) Erase(run runner.RunId) error {
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
func (s *Statuses) queryAndListen(q runner.StatusQuery, listen bool) (current []runner.ProcessStatus, future chan runner.ProcessStatus, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, runID := range q.Runs {
		st, ok := s.runs[runID]
		if !ok {
			return nil, nil, fmt.Errorf(UnknownRunIdMsg, runID)
		}
		if q.States.Matches(st) {
			current = append(current, st)
		}
	}

	if err != nil || len(current) > 0 || !listen {
		return current, nil, err
	}

	ch := make(chan runner.ProcessStatus, 1)
	s.listeners = append(s.listeners, queryAndCh{q: q, ch: ch})
	return nil, ch, nil
}
