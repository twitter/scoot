package scheduler

import (
	"fmt"
	"github.com/scootdev/scoot/async"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/worker"
)

// Scheduler that keeps track of the state of running tasks & the cluster
// so that it can make smarter scheduling decisions
//
// Scheduler Concurrency: The Scheduler runs an update loop in its own go routine.
// periodically the scheduler does some async work using async.Runner.  The async
// work is executed in its own Go routine, nothing in async functions should read
// or modify scheduler state directly.
//
// The callbacks are executed as part of the scheduler loop.  They therefore can
// safely read & modify the scheduler state.
type statefulScheduler struct {
	sagaCoord     saga.SagaCoordinator
	workerFactory worker.WorkerFactory
	asyncRunner   async.Runner
	addJobCh      chan jobAddedMsg

	// Scheduler State
	clusterState   *clusterState
	inProgressJobs map[string]*jobState // map of inprogress jobId to jobState

	// stats
	stats stats.StatsReceiver
}

func NewStatefulSchedulerFromCluster(
	cl *cluster.Cluster,
	sc saga.SagaCoordinator,
	wf worker.WorkerFactory,
	stats stats.StatsReceiver) Scheduler {
	sub := cl.Subscribe()
	return NewStatefulScheduler(
		sub.InitialMembers,
		sub.Updates,
		sc,
		wf,
		stats,
		false)
}

// Create a New StatefulScheduler that implements the Scheduler interface
// specifying debugMode true, starts the scheduler up but does not start
// the update loop.  Instead the loop must be advanced manulaly by calling
// step(), intended for debugging and test cases
func NewStatefulScheduler(
	initialCluster []cluster.Node,
	clusterUpdates chan []cluster.NodeUpdate,
	sc saga.SagaCoordinator,
	wf worker.WorkerFactory,
	stats stats.StatsReceiver,
	debugMode bool,
) *statefulScheduler {

	sched := &statefulScheduler{
		sagaCoord:     sc,
		workerFactory: wf,
		asyncRunner:   async.NewRunner(),
		addJobCh:      make(chan jobAddedMsg, 1),

		clusterState:   newClusterState(initialCluster, clusterUpdates),
		inProgressJobs: make(map[string]*jobState),

		stats: stats,
	}

	sched.startUp()

	if !debugMode {
		// start the scheduler loop
		go func() {
			sched.loop()
		}()
	}

	return sched
}

//
// Starts the scheduler, must be called before any other
// methods on the scheduler can be called
func (s *statefulScheduler) startUp() {
	// TODO: Recover form SagaLog Any In Process Tasks
	// Return only once all those have been scheduled
}

type jobAddedMsg struct {
	job  sched.Job
	saga *saga.Saga
}

func (s *statefulScheduler) ScheduleJob(job sched.Job) error {

	defer s.stats.Latency("schedJobLatency_ms").Time().Stop()
	s.stats.Counter("schedJobRequests").Inc(1)

	// Log StartSaga Message
	// TODO: need to serialize job into binary and pass in here
	// so we can recover the job in case of failure
	sagaObj, err := s.sagaCoord.MakeSaga(job.Id, nil)
	if err != nil {
		return err
	}

	s.addJobCh <- jobAddedMsg{
		job:  job,
		saga: sagaObj,
	}

	return nil
}

// run the scheduler loop indefinitely
func (s *statefulScheduler) loop() {
	for {
		s.step()
	}
}

// run one loop iteration
func (s *statefulScheduler) step() {
	//fmt.Printf("JobState %+v \n", s.inProgressJobs["0"])
	// update scheduler state with messages received since last loop
	// nodes added or removed to cluster, new jobs scheduled,
	// async functions completed & invoke callbacks
	s.addJobs()
	s.clusterState.updateCluster()
	s.asyncRunner.ProcessMessages()

	// TODO: make processUpdates on scheduler state wait until an update
	// has been received
	// instead of just burning CPU and constantly looping while no updates
	// have occurred

	s.checkForCompletedJobs()
	s.scheduleTasks()
}

// Checks if any new jobs have been scheduled since the last loop and adds
// them to the scheduler state
func (s *statefulScheduler) addJobs() {
	// Add New Jobs to State
	select {
	case newJobMsg := <-s.addJobCh:
		s.inProgressJobs[newJobMsg.job.Id] = newJobState(newJobMsg.job, newJobMsg.saga)
	default:
	}
}

// checks if any of the in progress jobs are completed.  If a job is
// completed log an EndSaga Message to the SagaLog asynchronously
func (s *statefulScheduler) checkForCompletedJobs() {

	// Check For Completed Jobs & Log EndSaga Message
	for _, jobState := range s.inProgressJobs {
		if jobState.getJobStatus() == sched.Completed && !jobState.EndingSaga {

			// mark job as being completed
			jobState.EndingSaga = true

			// set up variables for async functions for async function & callbacks
			j := jobState

			s.asyncRunner.RunAsync(
				func() error {
					return j.Saga.EndSaga()
				},
				func(err error) {
					if err == nil {
						fmt.Printf("Job %v Completed \n", j.Job.Id)
						// This job is fully processed remove from
						// InProgressJobs
						delete(s.inProgressJobs, j.Job.Id)
					} else {
						// set the jobState flag to false, will retry logging
						// EndSaga message on next scheduler loop
						j.EndingSaga = false
					}
				})
		}
	}
}

// figures out which tasks to schedule next and on which worker and then runs them
func (s *statefulScheduler) scheduleTasks() {
	// Get a list of all available tasks to be ran
	var unscheduledTasks []*taskState
	for _, jobState := range s.inProgressJobs {
		unscheduledTasks = append(unscheduledTasks, jobState.getUnScheduledTasks()...)
	}

	// Calculate a list of Tasks to Node Assignments & start running all those jobs
	taskAssignments := getTaskAssignments(s.clusterState, unscheduledTasks)
	for _, ta := range taskAssignments {

		// Set up variables for async functions & callback
		jobId := ta.task.JobId
		taskId := ta.task.TaskId
		taskDef := ta.task.Def
		saga := s.inProgressJobs[jobId].Saga
		wf := s.workerFactory(ta.node)
		jobState := s.inProgressJobs[jobId]

		// Mark Task as Started
		s.clusterState.taskScheduled(ta.node.Id(), taskId)
		jobState.taskStarted(taskId)

		s.asyncRunner.RunAsync(
			func() error {
				fmt.Println("Starting task", taskId)
				return runTaskAndLog(
					saga,
					wf,
					taskId,
					taskDef)
			},
			func(err error) {
				fmt.Println("Ending task", taskId)
				// update the jobState
				if err == nil {
					jobState.taskCompleted(taskId)
				} else {
					jobState.errorRunningTask(taskId, err)
				}

				// update cluster state that this node is now free
				s.clusterState.taskCompleted(ta.node.Id(), taskId)
			})
	}
}
