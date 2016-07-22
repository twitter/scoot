package scheduler

/*import (
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	ci "github.com/scootdev/scoot/sched/clusterimplementations"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	"testing"
)

func Test_ScheduleJob_WritingStartSagaFails(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	//nodeMock used to ensure no tasks are executed if StartSaga Fails
	nodeMock := cm.NewMockNode(mockCtrl)
	nodeMock.EXPECT().Id().Return("node1")
	cluster, clusterState := cm.DynamicClusterFactory([]cm.Node{nodeMock})

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil).Return(saga.NewInternalLogError("test error"))
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)

	scheduler := NewScheduler(cluster, clusterState, sagaCoord)

	job := sched.GenJob("job1", 5)
	err := scheduler.ScheduleJob(job)
	scheduler.BlockUnitlAllJobsCompleted()

	if err == nil {
		t.Error("Exepected Error to be Returned")
	}
}

func Test_ScheduleJob_JobsExecuteSuccessfully(t *testing.T) {

	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 20
	properties := gopter.NewProperties(parameters)

	properties.Property("Scheduled Jobs should Update Saga Log Correctly", prop.ForAll(
		func(jobId string, numTasks int16, numNodes int16) bool {

			cluster, clusterState := ci.DynamicLocalNodeClusterFactory(int(numNodes))
			sagaCoord := saga.MakeInMemorySagaCoordinator()
			scheduler := NewScheduler(cluster, clusterState, sagaCoord)

			job := sched.GenJob(jobId, int(numTasks))
			err := scheduler.ScheduleJob(job)

			if err != nil {
				fmt.Println("Unexpected Error Scheduling Job", err)
				return false
			}

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
		gen.Int16Range(100, 1000),
	))

	properties.TestingRun(t)
}
*/
