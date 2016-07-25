package scheduler

import (
	s "github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
)

type JobStatus struct {
	Id         string
	Status     sched.Status
	TaskStatus map[string]sched.Status //map of taskId to status
}

//
func GetJobStatus(jobId string, sc s.SagaCoordinator) (JobStatus, error) {
	state, err := sc.GetSagaState(jobId)

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
