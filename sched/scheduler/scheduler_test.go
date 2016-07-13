package scheduler

import (
	"fmt"
	//"github.com/golang/mock/gomock"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	ci "github.com/scootdev/scoot/sched/clusterimplementations"
	//cm "github.com/scootdev/scoot/sched/clustermembership"
	"testing"
)

func Test_ScheduleJob_PropertyTest(t *testing.T) {

	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10
	properties := gopter.NewProperties(parameters)

	properties.Property("Scheduled Jobs should Update Saga Log Correctly", prop.ForAll(
		func(jobId string, numTasks int16, numNodes int16) bool {

			cluster, clusterState := ci.DynamicLocalNodeClusterFactory(int(numNodes))
			sagaCoord := saga.MakeInMemorySagaCoordinator()
			scheduler := NewScheduler(cluster, clusterState, sagaCoord)

			job := sched.GenJob(jobId, int(numTasks))
			scheduler.ScheduleJob(job)
			scheduler.BlockUnitlAllJobsCompleted()

			saga, _ := sagaCoord.RecoverSagaState(job.Id, saga.ForwardRecovery)
			sagaState := saga.GetState()

			if !sagaState.IsSagaCompleted() {
				fmt.Println("Expected Job to be Completed")
				return false
			}

			if sagaState.IsSagaAborted() {
				fmt.Println("Expected Saga to Not be Aborted")
				return false
			}

			for taskId, _ := range job.Def.Tasks {
				if !sagaState.IsTaskStarted(taskId) {
					fmt.Println("Expected task to be started", taskId)
					return false
				}
				if !sagaState.IsTaskCompleted(taskId) {
					fmt.Println("Expected task to be compelted,", taskId)
					return false
				}

				if sagaState.IsCompTaskStarted(taskId) {
					fmt.Println("Expected Comp Task to not be started", taskId)
					return false
				}

				if sagaState.IsCompTaskCompleted(taskId) {
					fmt.Println("Expected Comp Task to not be competed", taskId)
					return false
				}
			}

			return true
		},
		sched.GenJobId(),
		gen.Int16Range(1, 100),
		gen.Int16Range(1, 1000),
	))

	properties.TestingRun(t)
}
