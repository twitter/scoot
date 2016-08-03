package scheduler

import (
	s "github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"sync"
)

type jobStates struct {
	// TODO(dbentley): remove inactive saga states
	// (Ideally after they've been idle for a while)
	actives map[string]*s.SagaState
	mu      sync.Mutex
}

func (st *jobStates) setState(id string, state *s.SagaState) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.actives[id] = state
}

func (st *jobStates) getState(id string) *s.SagaState {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.actives[id]
}

func (s *coordinator) GetState(jobId string) (*s.SagaState, error) {
	state := s.actives.getState(jobId)
	if state != nil {
		return state, nil
	}
	return s.sc.GetSagaState(jobId)
}

func (s *coordinator) GetStatus(jobId string) (JobStatus, error) {
	state, err := s.GetState(jobId)
	if err != nil {
		return JobStatus{}, err
	}

	// No Logged Saga Messages.  Job NotStarted yet
	if state == nil {
		js := JobStatus{
			Id:     jobId,
			Status: sched.NotStarted,
		}
		return js, nil
	}
	return convertSagaStateToJobStatus(state), nil
}

type JobStatus struct {
	Id         string
	Status     sched.Status
	TaskStatus map[string]sched.Status //map of taskId to status
}

// Converts a SagaState to a corresponding JobStatus
func convertSagaStateToJobStatus(sagaState *s.SagaState) JobStatus {

	jobStatus := JobStatus{
		Id:         sagaState.SagaId(),
		Status:     sched.NotStarted,
		TaskStatus: make(map[string]sched.Status),
	}

	// NotStarted Tasks will not have a logged value
	for _, id := range sagaState.GetTaskIds() {

		taskStatus := sched.NotStarted

		if sagaState.IsSagaAborted() {
			if sagaState.IsCompTaskCompleted(id) {
				taskStatus = sched.RolledBack
			} else if sagaState.IsTaskStarted(id) {
				taskStatus = sched.RollingBack
			}
		} else {
			if sagaState.IsTaskCompleted(id) {
				taskStatus = sched.Completed
			} else if sagaState.IsTaskStarted(id) {
				taskStatus = sched.InProgress
			}
		}

		jobStatus.TaskStatus[id] = taskStatus
	}

	// Saga Completed Successfully
	if sagaState.IsSagaCompleted() && !sagaState.IsSagaAborted() {
		jobStatus.Status = sched.Completed

		// Saga Completed Unsuccessfully was Aborted & Rolled Back
	} else if sagaState.IsSagaCompleted() && sagaState.IsSagaAborted() {
		jobStatus.Status = sched.RolledBack

		// Saga In Progress
	} else if !sagaState.IsSagaCompleted() && !sagaState.IsSagaAborted() {
		jobStatus.Status = sched.InProgress

		// Saga in Progress - Aborted and Rolling Back
	} else if !sagaState.IsSagaCompleted() && sagaState.IsSagaAborted() {
		jobStatus.Status = sched.RollingBack
	}

	return jobStatus
}
