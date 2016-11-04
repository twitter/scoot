package local

type Statuses struct {
	runs      map[runner.RunId]runner.ProcessStatus
	nextRunId int64
	mu        sync.Mutex

	Firehose chan runner.ProcessStatus
}

func (s *Statuses) Status(runId runner.RunId) (runner.ProcessStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.runs[runId]
	if !ok {
		return runner.ProcessStatus{}, fmt.Errorf("could not find: %v", runId)
	}
	return result, nil
}

func (s *Statuses) StatusAll() ([]runner.ProcessStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	statuses := []runner.ProcessStatus{}
	for _, status := range s.runs {
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func (s *Statuses) Update(newStatus runner.ProcessStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldStatus, ok := s.runs[newStatus.RunId]
	if !ok {
		return runner.ProcessStatus{}, fmt.Errorf("cannot find run %v", newStatus.RunId)
	}

	if oldStatus.State.IsDone() {
		return oldStatus, nil
	}

	if newStatus.StdoutRef == "" {
		newStatus.StdoutRef = oldStatus.StdoutRef
	}
	if newStatus.StderrRef == "" {
		newStatus.StderrRef = oldStatus.StderrRef
	}

	r.runs[newStatus.RunId] = newStatus
	s.Firehose <- newStatus
}
