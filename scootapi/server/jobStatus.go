package server

import (
	s "github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

func (h *Handler) GetStatus(jobId string) (*scoot.JobStatus, error) {
	state, err := h.s.GetState(jobId)

	if err != nil {
		js := scoot.NewJobStatus()
		js.ID = ""
		js.Status = scoot.Status_NOT_STARTED

		if err != nil {
			switch err.(type) {
			case s.InvalidRequestError:
				err = scoot.NewInvalidRequest()
			case s.InternalLogError:
				err = scoot.NewScootServerError()
			}
		}

		return js, err
	}

	// No Logged Saga Messages.  Job NotStarted yet
	if state == nil {
		js := scoot.NewJobStatus()
		js.ID = jobId
		js.Status = scoot.Status_NOT_STARTED

		return js, nil
	}

	return convertSagaStateToJobStatus(state), nil
}

// Converts a SagaState to a corresponding JobStatus
func convertSagaStateToJobStatus(sagaState *s.SagaState) *scoot.JobStatus {

	js := scoot.NewJobStatus()

	js.ID = sagaState.SagaId()
	js.Status = scoot.Status_NOT_STARTED
	js.TaskStatus = make(map[string]scoot.Status)

	// NotStarted Tasks will not have a logged value
	for _, id := range sagaState.GetTaskIds() {

		taskStatus := scoot.Status_NOT_STARTED

		if sagaState.IsSagaAborted() {
			if sagaState.IsCompTaskCompleted(id) {
				taskStatus = scoot.Status_ROLLED_BACK
			} else if sagaState.IsTaskStarted(id) {
				taskStatus = scoot.Status_ROLLING_BACK
			}
		} else {
			if sagaState.IsTaskCompleted(id) {
				taskStatus = scoot.Status_COMPLETED
			} else if sagaState.IsTaskStarted(id) {
				taskStatus = scoot.Status_IN_PROGRESS
			}
		}

		js.TaskStatus[id] = taskStatus
	}

	// Saga Completed Successfully
	if sagaState.IsSagaCompleted() && !sagaState.IsSagaAborted() {
		js.Status = scoot.Status_COMPLETED

		// Saga Completed Unsuccessfully was Aborted & Rolled Back
	} else if sagaState.IsSagaCompleted() && sagaState.IsSagaAborted() {
		js.Status = scoot.Status_ROLLED_BACK

		// Saga In Progress
	} else if !sagaState.IsSagaCompleted() && !sagaState.IsSagaAborted() {
		js.Status = scoot.Status_IN_PROGRESS

		// Saga in Progress - Aborted and Rolling Back
	} else if !sagaState.IsSagaCompleted() && sagaState.IsSagaAborted() {
		js.Status = scoot.Status_ROLLING_BACK
	}

	return js
}
