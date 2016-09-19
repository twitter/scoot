package server

import (
	"fmt"
	"github.com/scootdev/scoot/common/thrifthelpers"
	s "github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

func GetJobStatus(jobId string, sc s.SagaCoordinator) (*scoot.JobStatus, error) {
	state, err := sc.GetSagaState(jobId)

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
	js.TaskData = make(map[string]*scoot.RunStatus)

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
				setTaskData(js, id, sagaState.GetEndTaskData(id))
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

func setTaskData(js *scoot.JobStatus, id string, resultsFromSaga []byte) error {
	workerRunStatus := worker.RunStatus{}
	thrifthelpers.JsonDeserialize(&workerRunStatus, resultsFromSaga)

	scootRunStatus := scoot.RunStatus{}
	var err error
	scootRunStatus.Status, err = scoot.RunStatusStateFromString(workerRunStatus.Status.String())
	if err != nil {
		return err
	}

	scootRunStatus.RunId = fmt.Sprintf("%s", workerRunStatus.RunId)
	scootRunStatus.OutUri = workerRunStatus.OutUri
	scootRunStatus.ErrUri = workerRunStatus.ErrUri
	scootRunStatus.ExitCode = workerRunStatus.ExitCode
	scootRunStatus.Error = workerRunStatus.Error

	js.TaskData[id] = &scootRunStatus
	return nil
}
