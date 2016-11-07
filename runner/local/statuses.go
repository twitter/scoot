package local

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/scootdev/scoot/runner"
)

type queryAndCh struct {
	q  runner.StatusQuery
	ch chan runner.ProcessStatus
}

type Statuses struct {
	mu        sync.Mutex
	runs      map[runner.RunId]runner.ProcessStatus
	nextRunId int64
	listeners []queryAndCh
}

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

func (s *Statuses) Update(newStatus runner.ProcessStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateUnderLock(newStatus)
}

func (s *Statuses) StatusQuery(q runner.StatusQuery, poll runner.PollOpts) ([]runner.ProcessStatus, error) {
	if poll.Timeout == 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.queryUnderLock(q)
	}

	current, future, err := s.queryAndListen(q)
	if err != nil || len(current) > 0 {
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

	if len(statuses) > 1 {
		return runner.ProcessStatus{}, fmt.Errorf("StatusQuerySingle expected 1 result; got %d: %v", len(statuses), statuses)
	}

	return statuses[0], nil
}

func (s *Statuses) Status(id runner.RunId) (runner.ProcessStatus, error) {
	return s.StatusQuerySingle(runner.RunDone(id), runner.Current())
}

func (s *Statuses) updateUnderLock(newStatus runner.ProcessStatus) {
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

	s.notifyAndUpdateListeners(newStatus)
}

func (s *Statuses) notifyAndUpdateListeners(newStatus runner.ProcessStatus) {
	listeners := make([]queryAndCh, 0, len(s.listeners))
	for i, listener := range s.listeners {
		if listener.q.Matches(newStatus) {
			listener.ch <- newStatus
			close(listener.ch)
		} else {
			listeners = append(listeners, listener)
		}
	}
	s.listeners = listeners
}

func (s *Statuses) queryAndListen(q runner.StatusQuery) (current []runner.ProcessStatus, future chan runner.ProcessStatus, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.queryUnderLock(q)
	if err != nil || len(r) > 0 {
		return r, nil, err
	}

	ch := make(chan runner.ProcessStatus, 1)
	s.listeners = append(s.listeners, queryAndCh{q: q, ch: ch})
	return nil, ch, nil
}

func (s *Statuses) queryUnderLock(q runner.StatusQuery) ([]runner.ProcessStatus, error) {
	var result []runner.ProcessStatus

	for _, runID := range q.Runs {
		st, ok := s.runs[runID]
		if !ok {
			return nil, fmt.Errorf("cannot find run %v", runID)
		}
		if q.States.Matches(st) {
			result = append(result, st)
		}
	}

	return result, nil
}
