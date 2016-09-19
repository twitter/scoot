package server

import (
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/scootdev/scoot/common/thrifthelpers"
	s "github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"strings"
	"testing"
)

func Test_GetJobStatus_InternalLogError(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := s.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages("job1").Return(nil, s.NewInternalLogError("test error"))
	sagaCoord := s.MakeSagaCoordinator(sagaLogMock)

	status, err := GetJobStatus("job1", sagaCoord)
	if err == nil {
		t.Error("Expected error to be returned when SagaLog fails to retrieve messages")
	}

	switch err.(type) {
	case *scoot.ScootServerError:
	default:
		t.Error("Expected returned error to be ScootServerError", err)
	}

	if status.ID != "" || status.Status != scoot.Status_NOT_STARTED {
		t.Error("Expected Default JobStatus to be returned when error occurs")
	}
}

func Test_GetJobStatus_InvalidRequestError(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := s.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages("job1").Return(nil, s.NewInvalidRequestError("test error"))
	sagaCoord := s.MakeSagaCoordinator(sagaLogMock)

	status, err := GetJobStatus("job1", sagaCoord)
	if err == nil {
		t.Error("Expected error to be returned when SagaLog fails to retrieve messages")
	}

	switch err.(type) {
	case *scoot.InvalidRequest:
	default:
		t.Error("Expected returned error to be ScootServerError", err)
	}

	if status.ID != "" || status.Status != scoot.Status_NOT_STARTED {
		t.Error("Expected Default JobStatus to be returned when error occurs")
	}
}

func Test_GetJobStatus_NoSagaMessages(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := s.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages("job1").Return(nil, nil)
	sagaCoord := s.MakeSagaCoordinator(sagaLogMock)

	status, err := GetJobStatus("job1", sagaCoord)
	if err != nil {
		t.Error("Unexpected error returned", err)
	}

	if status.ID != "job1" && status.Status != scoot.Status_IN_PROGRESS {
		t.Error("Unexpected JobStatus Returned")
	}
}

func Test_ConvertSagaStateToJobStatus(t *testing.T) {

	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 1000
	properties := gopter.NewProperties(parameters)

	properties.Property("SagaState Converted To Job Status Correctly", prop.ForAll(
		func(state *s.SagaState) bool {

			jobStatus := convertSagaStateToJobStatus(state)

			// Verify JobId Set Correctly
			if state.SagaId() != jobStatus.ID {
				return false
			}

			// Verify JobStatus
			switch jobStatus.Status {
			case scoot.Status_COMPLETED:
				if !state.IsSagaCompleted() {
					fmt.Println("Job Status is Completed when Saga is not")
					return false
				}
				if state.IsSagaAborted() {
					fmt.Println("Job Status is Completed when Saga is Aborted")
					return false
				}

			case scoot.Status_IN_PROGRESS:
				if state.IsSagaCompleted() {
					fmt.Println("Job Status is InProgress when Saga is Completed")
					return false
				}
				if state.IsSagaAborted() {
					fmt.Println("Job Status is InProgress when Saga is Aborted")
					return false
				}

			case scoot.Status_ROLLED_BACK:
				if !state.IsSagaCompleted() {
					fmt.Println("Job Status is RolledBack but Saga is not Completed")
					return false
				}
				if !state.IsSagaAborted() {
					fmt.Println("Job Status is RolledBack but Saga is not Aborted")
					return false
				}

			case scoot.Status_ROLLING_BACK:
				if !state.IsSagaAborted() {
					fmt.Println("Job Status is RollingBack but the Saga is not Aborted")
					return false
				}
				if state.IsSagaCompleted() {
					fmt.Println("Job Status is RollingBack but the Saga is Completed")
					return false
				}

			case scoot.Status_NOT_STARTED:
				fmt.Println("Unexepected Job State Not Started")
				return false
			}

			// Verify TaskStatus
			for _, id := range state.GetTaskIds() {

				switch jobStatus.TaskStatus[id] {
				case scoot.Status_COMPLETED:
					if state.IsSagaAborted() {
						fmt.Println("Task Status is Completed but Saga is Aborted, Expected RolledBack", id)
						return false
					}

					if !state.IsTaskCompleted(id) {
						fmt.Println("Task Status is Completed but Saga Task is Not Completed, Expected InProgress", id)
						return false
					}

					runResultAsBytes := state.GetEndTaskData(id)
					if !validateRunResult(runResultAsBytes, id) {
						return false
					}

				case scoot.Status_IN_PROGRESS:
					if state.IsSagaAborted() {
						fmt.Println("Task Status is InProgress but Saga is Aborted, Expected RollingBack", id)
						return false
					}

					if state.IsTaskCompleted(id) {
						fmt.Println("Task Status is InProgress but Saga Task is Completed, Expected Completed", id)
						return false
					}

				case scoot.Status_ROLLED_BACK:
					if !state.IsSagaAborted() {
						fmt.Println("Task Status is Rolled Back but Saga is Not Aborted, Expected Completed", id)
						return false
					}

					if !state.IsCompTaskCompleted(id) {
						fmt.Println("Task Status is RolledBack but Saga has not completed the Comp Task, Expected RollingBack", id)
						return false
					}

				case scoot.Status_ROLLING_BACK:
					if !state.IsSagaAborted() {
						fmt.Println("Task Status is RollingBack but Saga is Not Aborted, Expected In Progress", id)
						return false
					}

					if state.IsCompTaskCompleted(id) {
						fmt.Println("Task Status is RollingBack but Saga has completed CompTask, Expected RolledBack", id)
					}
				case scoot.Status_NOT_STARTED:
					fmt.Println("Unexepected Task State Not Started", id)
					return false
				}
			}

			return true

		},
		s.GenSagaState(),
	))

	properties.TestingRun(t)
}

func validateRunResult(resultsAsByte []byte, taskId string) bool {
	runResults := scoot.RunStatus{}
	thrifthelpers.JsonDeserialize(&runResults, resultsAsByte)

	if runResults.RunId != taskId {
		fmt.Printf(fmt.Sprintf("Run ids didn't match. got: %s,  expected: %s\n", taskId, runResults.RunId))
		return false
	}
	if runResults.Status < scoot.RunStatusState_COMPLETE {
		fmt.Printf(fmt.Sprintf("Taskid: %s, Invalid run status: %v\n", taskId, runResults.Status))
		return false
	}
	if int(*runResults.ExitCode) != 0 && int(*runResults.ExitCode) != -1 {
		fmt.Printf(fmt.Sprintf("Taskid: %s, Invalid exit code: %d\n", taskId, runResults.ExitCode))
		return false
	}
	if !strings.Contains(*runResults.Error, "error ") {
		fmt.Printf(fmt.Sprintf("Taskid: %s, Invalid error string: %s\n", taskId, runResults.Error))
		return false
	}
	if !strings.Contains(*runResults.OutUri, "out URI ") {
		fmt.Printf(fmt.Sprintf("Taskid: %s, Invalid out URI: %s\n", taskId, runResults.OutUri))
		return false
	}
	if !strings.Contains(*runResults.ErrUri, "error URI ") {
		fmt.Printf(fmt.Sprintf("Taskid: %s, Invalid err URI: %s\n", taskId, runResults.ErrUri))
		return false
	}

	return true
}
