package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	uuid "github.com/nu7hatch/gouuid"

	"github.com/scootdev/scoot/async"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
)

// Scheduler Config variables read at initialization
// MaxRetriesPerTask - the number of times to retry a failing task before
//     marking it as completed.
// DebugMode - if true, starts the scheduler up but does not start
//     the update loop.  Instead the loop must be advanced manually
//     by calling step()
// RecoverJobsOnStartup - if true, the scheduler recovers active sagas,
//     from the sagalog, and restarts them.
// DefaultTaskTimeout -
//     default timeout for tasks, in ms.
// RunnerOverhead -
//     default overhead to add (to account for network and downloading).
// RunnerRetryTimeout -
//     how long to keep retrying a runner req.
// RunnerRetryInterval -
//     how long to sleep between runner req retries.
// ReadyFnBackoff -
//     how long to wait between runner status queries to determine [init] status.

type SchedulerConfig struct {
	MaxRetriesPerTask    int
	DebugMode            bool
	RecoverJobsOnStartup bool
	DefaultTaskTimeout   time.Duration
	RunnerOverhead       time.Duration
	RunnerRetryTimeout   time.Duration
	RunnerRetryInterval  time.Duration
	ReadyFnBackoff       time.Duration
}

type RunnerFactory func(node cluster.Node) runner.Service

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
	runnerFactory RunnerFactory
	asyncRunner   async.Runner
	addJobCh      chan jobAddedMsg
	killJobCh     chan jobKillRequest

	// Scheduler config
	maxRetriesPerTask   int
	defaultTaskTimeout  time.Duration
	runnerRetryTimeout  time.Duration
	runnerRetryInterval time.Duration
	runnerOverhead      time.Duration
	readyFnBackoff      time.Duration

	// Scheduler State
	clusterState   *clusterState
	inProgressJobs map[string]*jobState // map of inprogress jobId to jobState

	// stats
	stat stats.StatsReceiver
}

// contains jobId to be killed and callback for the result of processing the request
type jobKillRequest struct {
	jobId     string
	responeCh chan error
}

// Create a New StatefulScheduler that implements the Scheduler interface
// cluster.Cluster - cluster of worker nodes
// saga.SagaCoordinator - the Saga Coordinator to log to and recover from
// RunnerFactory - Function which converts a node to a Runner
// SchedulerConfig - additional configuration settings for the scheduler
// StatsReceiver - stats receiver to log statistics to
func NewStatefulSchedulerFromCluster(
	cl *cluster.Cluster,
	sc saga.SagaCoordinator,
	rf RunnerFactory,
	config SchedulerConfig,
	stat stats.StatsReceiver,
) Scheduler {
	sub := cl.Subscribe()
	return NewStatefulScheduler(
		sub.InitialMembers,
		sub.Updates,
		sc,
		rf,
		config,
		stat,
	)
}

// Create a New StatefulScheduler that implements the Scheduler interface
// specifying debugMode true, starts the scheduler up but does not start
// the update loop.  Instead the loop must be advanced manulaly by calling
// step(), intended for debugging and test cases
// If recoverJobsOnStartup is true Active Sagas in the saga log will be recovered
// and rescheduled, otherwise no recovery will be done on startup
func NewStatefulScheduler(
	initialCluster []cluster.Node,
	clusterUpdates chan []cluster.NodeUpdate,
	sc saga.SagaCoordinator,
	rf RunnerFactory,
	config SchedulerConfig,
	stat stats.StatsReceiver,
) *statefulScheduler {

	nodeReadyFn := func(node cluster.Node) (bool, time.Duration) {
		run := rf(node)
		st, svc, err := run.StatusAll()
		if err != nil || !svc.Initialized {
			if svc.Error != nil {
				log.Info("Received service err during init of new node: %v, err: %v", node, svc.Error)
				return false, 0
			}
			return false, config.ReadyFnBackoff
		}
		for _, s := range st {
			log.Info("Aborting existing run on new node: ", node, s)
			run.Abort(s.RunID)
		}
		return true, 0
	}
	if config.ReadyFnBackoff == 0 {
		nodeReadyFn = nil
	}

	sched := &statefulScheduler{
		sagaCoord:     sc,
		runnerFactory: rf,
		asyncRunner:   async.NewRunner(),
		addJobCh:      make(chan jobAddedMsg, 1),
		killJobCh:     make(chan jobKillRequest, 1),

		maxRetriesPerTask:   config.MaxRetriesPerTask,
		defaultTaskTimeout:  config.DefaultTaskTimeout,
		runnerRetryTimeout:  config.RunnerRetryTimeout,
		runnerRetryInterval: config.RunnerRetryInterval,
		runnerOverhead:      config.RunnerOverhead,

		clusterState:   newClusterState(initialCluster, clusterUpdates, nodeReadyFn),
		inProgressJobs: make(map[string]*jobState),
		stat:           stat,
	}

	if !config.DebugMode {
		// start the scheduler loop
		go func() {
			sched.loop()
		}()
	}

	// Recover Jobs in a separate go routine to allow the scheduler
	// to accept new jobs while recovering old ones.
	if config.RecoverJobsOnStartup {
		go func() {
			recoverJobs(sched.sagaCoord, sched.addJobCh)
		}()
	}
	return sched
}

type jobAddedMsg struct {
	job  *sched.Job
	saga *saga.Saga
}

func (s *statefulScheduler) ScheduleJob(jobDef sched.JobDefinition) (string, error) {
	defer s.stat.Latency("schedJobLatency_ms").Time().Stop()
	s.stat.Counter("schedJobRequestsCounter").Inc(1)

	job := &sched.Job{
		Id:  generateJobId(),
		Def: jobDef,
	}

	asBytes, err := job.Serialize()
	if err != nil {
		return "", err
	}

	// Log StartSaga Message
	sagaObj, err := s.sagaCoord.MakeSaga(job.Id, asBytes)
	if err != nil {
		return "", err
	}

	s.stat.Counter("schedJobsCounter").Inc(1)
	s.addJobCh <- jobAddedMsg{
		job:  job,
		saga: sagaObj,
	}

	return job.Id, nil
}

// generates a jobId using a random uuid
func generateJobId() string {

	// uuid.NewV4() should never actually return an error the code uses
	// rand.Read Api to generate the uuid, which according to golang docs
	// "Read always returns ... a nil error" https://golang.org/pkg/math/rand/#Read
	for {
		if id, err := uuid.NewV4(); err == nil {
			return id.String()
		}
	}
}

// run the scheduler loop indefinitely
func (s *statefulScheduler) loop() {
	for {
		s.step()
		remaining := 0
		for _, job := range s.inProgressJobs {
			remaining += (len(job.Tasks) - job.TasksCompleted)
		}
		s.stat.Gauge("schedInProgressJobsGauge").Update(int64(len(s.inProgressJobs)))
		s.stat.Gauge("schedInProgressTasksGauge").Update(int64(remaining))
		s.stat.Gauge("schedNumRunningTasksGauge").Update(int64(s.asyncRunner.NumRunning()))
		time.Sleep(50 * time.Millisecond) // TODO(jschiller): find a better way to avoid pegging the cpu.
	}
}

// run one loop iteration
func (s *statefulScheduler) step() {
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
	s.killJobs()
	s.scheduleTasks()
}

// Checks if any new jobs have been scheduled since the last loop and adds
// them to the scheduler state
func (s *statefulScheduler) addJobs() {
	select {
	case newJobMsg := <-s.addJobCh:
		s.inProgressJobs[newJobMsg.job.Id] = newJobState(newJobMsg.job, newJobMsg.saga)

		var total, completed, running int
		for _, job := range s.inProgressJobs {
			total += len(job.Tasks)
			completed += job.TasksCompleted
			running += job.TasksRunning
		}
		log.Infof("Created new Job: %s with %d tasks. Now: tasks unscheduled: %d, running: %d, completed: %d, total: %d",
			newJobMsg.job.Id, len(newJobMsg.job.Def.Tasks), total-completed-running, running, completed, total)
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
						log.Infof("Job completed and logged: %v", j.Job.Id)
						// This job is fully processed remove from
						// InProgressJobs
						delete(s.inProgressJobs, j.Job.Id)
					} else {
						// set the jobState flag to false, will retry logging
						// EndSaga message on next scheduler loop
						j.EndingSaga = false
						s.stat.Counter("schedRetriedEndSagaCounter").Inc(1)
						log.Infof("Job completed but failed to log: %v", j.Job.Id)
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
	if len(unscheduledTasks) == 0 {
		return
	}

	// Calculate a list of Tasks to Node Assignments & start running all those jobs
	taskAssignments, nodeGroups := getTaskAssignments(s.clusterState, unscheduledTasks)
	s.clusterState.nodeGroups = nodeGroups
	for _, ta := range taskAssignments {

		// Set up variables for async functions & callback
		jobId := ta.task.JobId
		taskId := ta.task.TaskId
		taskDef := ta.task.Def
		jobState := s.inProgressJobs[jobId]
		sa := s.inProgressJobs[jobId].Saga
		nodeId := ta.node.Id()

		preventRetries := bool(ta.task.NumTimesTried >= s.maxRetriesPerTask)

		// Mark Task as Started in the cluster
		s.clusterState.taskScheduled(nodeId, taskId, taskDef.SnapshotID)
		log.Infof("job:%s, task:%s, scheduled on node:%s\n", jobId, taskId, nodeId)

		tRunner := &taskRunner{
			saga:   sa,
			runner: s.runnerFactory(ta.node),
			stat:   s.stat,

			defaultTaskTimeout:    s.defaultTaskTimeout,
			runnerRetryTimeout:    s.runnerRetryTimeout,
			runnerRetryInterval:   s.runnerRetryInterval,
			runnerOverhead:        s.runnerOverhead,
			markCompleteOnFailure: preventRetries,

			jobId:  jobId,
			taskId: taskId,
			task:   taskDef,
			nodeId: nodeId,

			abortCh: make(chan interface{}, 1),
		}

		// mark the task as started in the jobState and record its taskRunner
		jobState.taskStarted(taskId, tRunner)

		s.asyncRunner.RunAsync(
			tRunner.run,
			func(err error) {
				log.Infof("In task callback for job: Error:%+v", err)
				flaky := false
				if err != nil {
					// Get the type of error. Currently we only care to distinguish runner (ex: thrift) errors to mark flaky nodes.
					taskErr := err.(*taskError)
					st := err.(*taskError).st
					aborted := (st.State == runner.ABORTED)
					flaky = (taskErr.runnerErr != nil)

					msg := "Error running job (will be retried):"
					if aborted {
						msg = "Task aborted."
					}
					if !aborted {
						if preventRetries {
							msg = fmt.Sprintf("Error running job (quitting, hit max retries of %d):", s.maxRetriesPerTask)
							err = nil
						} else {
							jobState.errorRunningTask(taskId, err)
						}
					}
					log.Info(msg, jobId, ", task:", taskId, " err:", taskErr, " cmd:", taskDef.Argv)

					// If the task completed succesfully but sagalog failed, start a goroutine to retry until it succeeds.
					if taskErr.sagaErr != nil && taskErr.runnerErr == nil && taskErr.resultErr == nil {
						log.Info(msg, jobId, ", task:", taskId, " -> starting goroutine to handle failed saga.EndTask. ")
						//TODO -this may results in closed channel panic due to sending endSaga to sagalog (below) before endTask
						go func() {
							for err := errors.New(""); err != nil; err = tRunner.logTaskStatus(&taskErr.st, saga.EndTask) {
								time.Sleep(time.Second)
							}
							log.Info(msg, jobId, ", task:", taskId, " -> finished goroutine to handle failed saga.EndTask. ")
						}()
					}
					if aborted {
						jobState.taskCompleted(taskId)
					}
				}
				if err == nil {
					log.Info("Ending job:", jobId, ", task:", taskId, " command:", strings.Join(taskDef.Argv, " "))
					jobState.taskCompleted(taskId)
				}

				// update cluster state that this node is now free and if we consider the runner to be flaky.
				log.Info("Freeing node:", nodeId, ", removed job:", jobId, ", task:", taskId)
				s.clusterState.taskCompleted(nodeId, taskId, flaky)

				total := 0
				completed := 0
				running := 0
				for _, job := range s.inProgressJobs {
					total += len(job.Tasks)
					completed += job.TasksCompleted
					running += job.TasksRunning
				}
				log.Info("Job:", jobState.Job.Id, " #running:", jobState.TasksRunning, " #completed:", jobState.TasksCompleted,
					" #total:", len(jobState.Tasks), " isdone:", (jobState.TasksCompleted == len(jobState.Tasks)))
				log.Info("Jobs summary -> running:", running, " completed:", completed, " total:", total, " alldone:", (completed == total))
			})
	}
}

/**
Put the kill request on channel that is processed by the main
scheduler loop, and wait for the response
*/
func (s *statefulScheduler) KillJob(jobId string) error {

	responseCh := make(chan error, 1)
	req := jobKillRequest{jobId: jobId, responeCh: responseCh}
	s.killJobCh <- req

	return <-req.responeCh

}

// process all requests verifying that the jobIds exist:  Send errors back
// immediately on the request channel for jobId that don't exist, then
// kill all the jobs with a valid ID
//
// this function is part of the main scheduler loop
func (s *statefulScheduler) killJobs() {
	var validKillRequests []jobKillRequest

	// validate jobids and sending invalid ids back and building a list of valid ids
	for haveKillRequest := true; haveKillRequest == true; {
		select {
		case req := <-s.killJobCh:
			// can we find the job?
			_, ok := s.inProgressJobs[req.jobId]
			if !ok {
				req.responeCh <- fmt.Errorf("Job Id %s, not found. "+
					" The job may be finished, "+
					" the request may still be in the queue to be scheduled, or "+
					" the id may be invalid.  "+
					" Check the job status, verify the id and/or resubmit the kill request after a few moments.",
					req.jobId)
			} else {
				validKillRequests = append(validKillRequests[:], req)
			}
		default:
			haveKillRequest = false
		}
	}

	// kill the jobs with valid ids
	for _, req := range validKillRequests {
		jobState := s.inProgressJobs[req.jobId]
		for _, task := range jobState.Tasks {
			if task.Status != sched.Completed {
				if task.TaskRunner != nil {
					task.TaskRunner.abortCh <- 1
				}
			}
		}

		req.responeCh <- nil
	}
}
