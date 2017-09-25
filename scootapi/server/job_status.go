package server

import (
	"github.com/twitter/scoot/common/thrifthelpers"
	s "github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
	"github.com/twitter/scoot/workerapi/gen-go/worker"
)

func GetJobStatus(jobId string, sc s.SagaCoordinator) (*scoot.JobStatus, error) {
	state, err := sc.GetSagaState(jobId)

	if err != nil {
		js := scoot.NewJobStatus()
		js.ID = ""
		js.Status = scoot.Status_NOT_STARTED

		switch err.(type) {
		case s.InvalidRequestError:
			err = scoot.NewInvalidRequest()
		case s.InternalLogError:
			err = scoot.NewScootServerError()
		}

		return js, err
	}

	// No Logged Saga Messages.  Job NotStarted yet
	if state == nil {
		js := scoot.NewJobStatus()
		js.ID = jobId
		js.Status = scoot.Status_NOT_STARTED
		js.TaskStatus = make(map[string]scoot.Status)
		js.TaskData = make(map[string]*scoot.RunStatus)

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
	if job, err := sched.DeserializeJob(sagaState.Job()); err == nil {
		for i, _ := range job.Def.Tasks {
			js.TaskStatus[job.Def.Tasks[i].TaskID] = scoot.Status_NOT_STARTED
		}
	}

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
				if thriftJobStatus, err := workerRunStatusToScootRunStatus(sagaState.GetEndTaskData(id)); err == nil {
					js.TaskData[id] = thriftJobStatus
				}
			} else if sagaState.IsTaskStarted(id) {
				taskStatus = scoot.Status_IN_PROGRESS
				if startData := sagaState.GetStartTaskData(id); startData != nil {
					if thriftJobStatus, err := workerRunStatusToScootRunStatus(startData); err == nil {
						js.TaskData[id] = thriftJobStatus
					}
				}

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

// this is a thrift to thrift structure translation.  We are doing this because we get invalid
// import statements in the generated code when we use thrift import statements (this issue is supposed
// to be fixed in thrift 10.0
func workerRunStatusToScootRunStatus(resultsFromSaga []byte) (*scoot.RunStatus, error) {
	if resultsFromSaga == nil {
		return nil, nil
	}

	workerRunStatus := worker.RunStatus{}
	thrifthelpers.JsonDeserialize(&workerRunStatus, resultsFromSaga)

	status, err := scoot.RunStatusStateFromString(workerRunStatus.Status.String())
	if err != nil {
		return nil, err
	}

	scootRunStatus := scoot.RunStatus{
		RunId:      workerRunStatus.RunId,
		Status:     status,
		OutUri:     workerRunStatus.OutUri,
		ErrUri:     workerRunStatus.ErrUri,
		ExitCode:   workerRunStatus.ExitCode,
		Error:      workerRunStatus.Error,
		SnapshotId: workerRunStatus.SnapshotId,
	}

	return &scootRunStatus, nil
}
