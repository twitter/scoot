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
	saga        saga.Saga
	distributor *dist.PoolDistributor
	started     bool           // tracks if Scheduler has been Initialized
	wg          sync.WaitGroup // used to track jobs in progress
}

func NewScheduler(cluster cm.DynamicCluster, clusterState cm.DynamicClusterState, saga saga.Saga) *scheduler {
	return &scheduler{
		cluster:     cluster,
		saga:        saga,
		distributor: dist.NewDynamicPoolDistributor(clusterState),
		started:     false,
	}
}

// Blocks until all scheduled jobs are compeleted
func (s *scheduler) BlockUnitlAllJobsCompleted() {
	s.wg.Wait()
}

// Starts the scheduler, must be called before any other
// methods on the scheduler can be called
func (s *scheduler) Start() {

	// Recover form SagaLog Any In Process Tasks
	// Return only once all those have been scheduled

	s.started = true
}

// Schedule a job, returns once the job has been successfully
// scheduled, nodes reserved & durably started Saga,
// Returns an error if scheduling was unsuccessful
func (s *scheduler) ScheduleJob(job sched.Job) error {

	if !s.started {
		return newUninitializedSchedError()
	}

	// Log StartSaga Message
	// TODO: need to serialize job into binary and pass in here
	// so we can recover the job in case of failure
	sagaState, err := s.saga.StartSaga(job.Id, nil)

	// If we succssfully started the Saga, ProcssJob
	if err == nil {

		// Reserve Nodes to Schedule Job On
		numNodes := getNumNodes(job)
		nodes := make([]cm.Node, 0, numNodes)
		for i := 0; i < numNodes; i++ {
			n := s.distributor.ReserveNode()
			nodes = append(nodes, n)
		}

		// Start Running Job
		s.wg.Add(1)
		go func(job sched.Job, sagaState *saga.SagaState, nodes []cm.Node) {

			jr := NewJobRunner(job, s.saga, sagaState, nodes)
			jr.runJob()

			// Release all nodes used for this job
			for _, node := range nodes {
				s.distributor.ReleaseNode(node)
			}

			s.wg.Done()
		}(job, sagaState, nodes)
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

type UninitializedSchedError struct {
	initialized bool
}

func newUninitializedSchedError() UninitializedSchedError {
	return UninitializedSchedError{}
}

func (e UninitializedSchedError) Error() string {
	return "Must Initialize sched by calling start() before any other methods can be executed"
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
