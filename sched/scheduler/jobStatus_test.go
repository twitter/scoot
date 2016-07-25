package scheduler

import (
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	s "github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"testing"
)

func Test_GetJobStatus_Error(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := s.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages("job1").Return(nil, s.NewInternalLogError("test error"))
	sagaCoord := s.MakeSagaCoordinator(sagaLogMock)

	status, err := GetJobStatus("job1", sagaCoord)
	if err == nil {
		t.Errorf("Expected error to be returned when SagaLog fails to retrieve messages")
	}
	if status.Id != "" || status.Status != sched.NotStarted {
		t.Errorf("Expected Default JobStatus to be returned when error occurs")
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

	if status.Id != "job1" && status.Status != sched.InProgress {
		t.Error("Unexpected JobStatus Returned")
	}
}

func Test_ConvertSagaStateToJobStatus(t *testing.T) {

	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)

	properties.Property("SagaState Converted To Job Status Correctly", prop.ForAll(
		func(state *s.SagaState) bool {

			jobStatus := convertSagaStateToJobStatus(state)

			// Verify JobId Set Correctly
			if state.SagaId() != jobStatus.Id {
				return false
			}

			// Verify JobStatus
			switch jobStatus.Status {
			case sched.Completed:
				if !state.IsSagaCompleted() {
					fmt.Println("Job Status is Completed when Saga is not")
					return false
				}
				if state.IsSagaAborted() {
					fmt.Println("Job Status is Completed when Saga is Aborted")
					return false
				}

			case sched.InProgress:
				if state.IsSagaCompleted() {
					fmt.Println("Job Status is InProgress when Saga is Completed")
					return false
				}
				if state.IsSagaAborted() {
					fmt.Println("Job Status is InProgress when Saga is Aborted")
					return false
				}

			case sched.RolledBack:
				if !state.IsSagaCompleted() {
					fmt.Println("Job Status is RolledBack but Saga is not Completed")
					return false
				}
				if !state.IsSagaAborted() {
					fmt.Println("Job Status is RolledBack but Saga is not Aborted")
					return false
				}

			case sched.RollingBack:
				if !state.IsSagaAborted() {
					fmt.Println("Job Status is RollingBack but the Saga is not Aborted")
					return false
				}
				if state.IsSagaCompleted() {
					fmt.Println("Job Status is RollingBack but the Saga is Completed")
					return false
				}

			case sched.NotStarted:
				fmt.Println("Unexepected Job State Not Started")
				return false
			}

			// Verify TaskStatus
			for _, id := range state.GetTaskIds() {

				switch jobStatus.TaskStatus[id] {
				case sched.Completed:
					if state.IsSagaAborted() {
						fmt.Println("Task Status is Completed but Saga is Aborted, Expected RolledBack", id)
						return false
					}

					if !state.IsTaskCompleted(id) {
						fmt.Println("Task Status is Completed but Saga Task is Not Completed, Expected InProgress", id)
						return false
					}

				case sched.InProgress:
					if state.IsSagaAborted() {
						fmt.Println("Task Status is InProgress but Saga is Aborted, Expected RollingBack", id)
						return false
					}

					if state.IsTaskCompleted(id) {
						fmt.Println("Task Status is InProgress but Saga Task is Completed, Expected Completed", id)
						return false
					}

				case sched.RolledBack:
					if !state.IsSagaAborted() {
						fmt.Println("Task Status is Rolled Back but Saga is Not Aborted, Expected Completed", id)
						return false
					}

					if !state.IsCompTaskCompleted(id) {
						fmt.Println("Task Status is RolledBack but Saga has not completed the Comp Task, Expected RollingBack", id)
						return false
					}

				case sched.RollingBack:
					if !state.IsSagaAborted() {
						fmt.Println("Task Status is RollingBack but Saga is Not Aborted, Expected In Progress", id)
						return false
					}

					if state.IsCompTaskCompleted(id) {
						fmt.Println("Task Status is RollingBack but Saga has completed CompTask, Expected RolledBack", id)
					}
				case sched.NotStarted:
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
