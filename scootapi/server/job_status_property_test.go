// +build property_test

package server

import (
	"fmt"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"

	s "github.com/scootdev/scoot/saga"

	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

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
		// Don't include a random job def.
		s.GenSagaState(false),
	))

	properties.TestingRun(t)
}
