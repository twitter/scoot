package scheduler

//go:generate mockgen -source=scheduler.go -package=scheduler -destination=scheduler_mock.go

import (
	"sync"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	dist "github.com/scootdev/scoot/sched/distributor"
	"github.com/scootdev/scoot/sched/worker"
)

type Scheduler interface {
	ScheduleJob(job sched.Job) error
}

type scheduler struct {
	sc            saga.SagaCoordinator
	nodes         *dist.PoolDistributor
	wg            sync.WaitGroup // used to track jobs in progress
	workerFactory worker.WorkerFactory
	stat          stats.StatsReceiver
}

func NewScheduler(
	nodes *dist.PoolDistributor,
	sc saga.SagaCoordinator,
	workerFactory worker.WorkerFactory,
	stat stats.StatsReceiver,
) *scheduler {
	s := &scheduler{
		nodes:         nodes,
		sc:            sc,
		workerFactory: workerFactory,
		stat:          stat,
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
// Should be used only for testing to verify expected
// tasks have been completed
func (s *scheduler) BlockUntilAllJobsCompleted() {
	s.wg.Wait()
}

// Schedule a job, returns once the job has been successfully
// scheduled, nodes reserved & durably started Saga,
// Returns an error if scheduling was unsuccessful
func (s *scheduler) ScheduleJob(job sched.Job) error {
	defer s.stat.Latency("schedJobLatency_ms").Time().Stop()
	s.stat.Counter("schedJobRequests").Inc(1)

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
		nodes := make([]cluster.Node, numNodes)
		for i := 0; i < numNodes; i++ {
			select {
			case nodes[i] = <-s.nodes.Reserve:
				continue
				// default:
				// 	//FIXME: better logic for handling case where total # of nodes is < numNodes.
				//  //       this code causes scheduler_test.go to block indefinitely...
				// 	numNodes = i
				// 	nodes = nodes[:i]
			}
		}
		jobNodes := dist.NewPoolDistributor(nodes, nil)

		// Start Running Job
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			runJob(job, sagaObj, jobNodes, s.workerFactory)
			jobNodes.Close()

			// Release all nodes used for this job
			for _, node := range nodes {
				s.nodes.Release <- node
			}
		}()
	}

	return err
}

// Get the Number of Nodes needed to run this job.  Right now this is
// dumb, and just returns min(len(Tasks), 5)
// TODO: Make this smarter
func getNumNodes(job sched.Job) int {
	numTasks := len(job.Def.Tasks)
	if numTasks > 5 {
		return 5
	} else {
		return numTasks
	}
}
