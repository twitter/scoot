package scheduler

import (
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	dist "github.com/scootdev/scoot/sched/distributor"
	"sync"
)

type scheduler struct {
	cluster     cm.DynamicCluster
	sc          saga.SagaCoordinator
	distributor *dist.PoolDistributor
	wg          sync.WaitGroup // used to track jobs in progress
}

func NewScheduler(cluster cm.DynamicCluster, clusterState cm.DynamicClusterState, sc saga.SagaCoordinator) *scheduler {
	s := &scheduler{
		cluster:     cluster,
		sc:          sc,
		distributor: dist.NewDynamicPoolDistributor(clusterState),
	}

	s.startUp()
	return s
}

// Starts the scheduler, must be called before any other
// methods on the scheduler can be called
func (s *scheduler) startUp() {

	// TODO: Recover form SagaLog Any In Process Tasks
	// Return only once all those have been scheduled

}

// Blocks until all scheduled jobs are compeleted
func (s *scheduler) BlockUnitlAllJobsCompleted() {
	s.wg.Wait()
}

// Schedule a job, returns once the job has been successfully
// scheduled, nodes reserved & durably started Saga,
// Returns an error if scheduling was unsuccessful
func (s *scheduler) ScheduleJob(job sched.Job) error {

	// Log StartSaga Message
	// TODO: need to serialize job into binary and pass in here
	// so we can recover the job in case of failure
	sagaObj, err := s.sc.MakeSaga(job.Id, nil)

	// If we succssfully started the Saga, ProcssJob
	if err == nil {

		// Reserve Nodes to Schedule Job On
		// By reserving nodes per Job before returnig
		// we get BackPressure & DataLocality within the job.
		numNodes := getNumNodes(job)
		nodes := make([]cm.Node, 0, numNodes)
		for i := 0; i < numNodes; i++ {
			n := s.distributor.ReserveNode()
			nodes = append(nodes, n)
		}

		// Start Running Job
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			runJob(job, sagaObj, nodes)

			// Release all nodes used for this job
			for _, node := range nodes {
				s.distributor.ReleaseNode(node)
			}
		}()
	}

	return err
}

// Get the Number of Nodes needed to run this job.  Right now this is
// dumb, and just returns min(len(Tasks), 5)
// TODO: Make this smarter
func getNumNodes(job sched.Job) int {
	numTasks := len(job.Tasks)
	if numTasks > 5 {
		return 5
	} else {
		return numTasks
	}
}

type InvalidJobError struct {
	msg string
}

func newInvalidJobError(msg string) InvalidJobError {
	return InvalidJobError{msg: msg}
}

func (e InvalidJobError) Error() string {
	return e.msg
}
