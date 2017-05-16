package scheduler

import (
	"errors"
	"fmt"
	"math"
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

//TODO(jschiller): these all temporary and need to to be configurable from CLI.

// Number of different requestors that can run jobs at any given time.
var MaxRequestors = 100

// Number of jobs any single requestor can have.
var MaxJobsPerRequestor = 100

// Expected number of nodes. Should roughly correspond with the actual number of healthy nodes.
var NumConfiguredNodes = 2730

// A reasonable maximum number of tasks we'd expect to queue.
var MaxSchedulableTasks = 50000

// Used to calculate how many tasks a job can run without adversely affecting other jobs.
var NodeScaleFactor = float32(NumConfiguredNodes) / float32(MaxSchedulableTasks)

// The maximum number of nodes required to run a 'large' job in an acceptable amount of time.
var LargeJobMaxNodes = 250

// Helpers.
func min(num int, nums ...int) int {
	m := num
	for _, n := range nums {
		if n < m {
			m = n
		}
	}
	return m
}
func max(num int, nums ...int) int {
	m := num
	for _, n := range nums {
		if n > m {
			m = n
		}
	}
	return m
}
func ceil(num float32) int {
	return int(math.Ceil(float64(num)))
}

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
	checkJobCh    chan jobCheckMsg
	addJobCh      chan jobAddedMsg

	// Scheduler config
	maxRetriesPerTask   int
	defaultTaskTimeout  time.Duration
	runnerRetryTimeout  time.Duration
	runnerRetryInterval time.Duration
	runnerOverhead      time.Duration
	readyFnBackoff      time.Duration

	// Scheduler State
	clusterState   *clusterState
	inProgressJobs []*jobState            // ordered list of inprogress jobId to job.
	requestorMap   map[string][]*jobState // map of requestor to all its jobs. Default requestor="" is ok.

	// stats
	stat stats.StatsReceiver
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
		checkJobCh:    make(chan jobCheckMsg, 1),
		addJobCh:      make(chan jobAddedMsg, 1),

		maxRetriesPerTask:   config.MaxRetriesPerTask,
		defaultTaskTimeout:  config.DefaultTaskTimeout,
		runnerRetryTimeout:  config.RunnerRetryTimeout,
		runnerRetryInterval: config.RunnerRetryInterval,
		runnerOverhead:      config.RunnerOverhead,

		clusterState:   newClusterState(initialCluster, clusterUpdates, nodeReadyFn),
		inProgressJobs: make([]*jobState, 0),
		requestorMap:   make(map[string][]*jobState),
		stat:           stat,
	}

	if !config.DebugMode {
		// start the scheduler loop
		log.Info("Starting scheduler loop")
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

type jobCheckMsg struct {
	jobDef   sched.JobDefinition
	resultCh chan error
}

type jobAddedMsg struct {
	job  *sched.Job
	saga *saga.Saga
}

func (s *statefulScheduler) ScheduleJob(jobDef sched.JobDefinition) (string, error) {
	defer s.stat.Latency("schedJobLatency_ms").Time().Stop()
	s.stat.Counter("schedJobRequestsCounter").Inc(1)
	log.Infof("Job request: Requestor:%s, Tag:%s, Basis:%s, Priority:%d, numTasks: %d",
		jobDef.Requestor, jobDef.Tag, jobDef.Basis, jobDef.Priority, len(jobDef.Tasks))

	checkResultCh := make(chan error, 1)
	s.checkJobCh <- jobCheckMsg{
		jobDef:   jobDef,
		resultCh: checkResultCh,
	}
	err := <-checkResultCh
	if err != nil {
		log.Infof("Rejected job request: %v -> %v", jobDef, err)
		return "", err
	}

	job := &sched.Job{
		Id:  generateJobId(),
		Def: jobDef,
	}
	if job.Def.Tag == "" {
		job.Def.Tag = job.Id
	}

	asBytes, err := job.Serialize()
	if err != nil {
		log.Infof("Failed to serialize job request: %v -> %v", jobDef, err)
		return "", err
	}

	// Log StartSaga Message
	sagaObj, err := s.sagaCoord.MakeSaga(job.Id, asBytes)
	if err != nil {
		log.Infof("Failed to create saga for job request: %v -> %v", jobDef, err)
		return "", err
	}

	log.Infof("Queueing job request: %v", jobDef)
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
		time.Sleep(250 * time.Millisecond)
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
	s.scheduleTasks()
}

// Checks if any new jobs have been scheduled since the last loop and adds
// them to the scheduler state
// TODO(jschiller): kill current jobs which share the same Requestor and Basis as these new jobs.
// TODO(jschiller): kill current tasks that share the same Requestor and Tag as these new tasks.
func (s *statefulScheduler) addJobs() {
checkLoop:
	for {
		select {
		case checkJobMsg := <-s.checkJobCh:
			if jobs, ok := s.requestorMap[checkJobMsg.jobDef.Requestor]; !ok && len(s.requestorMap) >= MaxRequestors {
				err := fmt.Errorf("Exceeds max number of requestors: %s (%d)", checkJobMsg.jobDef.Requestor, MaxRequestors)
				checkJobMsg.resultCh <- err
			} else if len(jobs) >= MaxJobsPerRequestor {
				err := fmt.Errorf("Exceeds max jobs per requestor: %s (%d)", checkJobMsg.jobDef.Requestor, MaxJobsPerRequestor)
				checkJobMsg.resultCh <- err
			} else if checkJobMsg.jobDef.Priority < sched.P0 || checkJobMsg.jobDef.Priority > sched.P3 {
				err := fmt.Errorf("Invalid priority %d, must be between 0-3 inclusive", checkJobMsg.jobDef.Priority)
				checkJobMsg.resultCh <- err
			} else {
				checkJobMsg.resultCh <- nil
			}
		default:
			break checkLoop
		}
	}

	receivedJob := false
	var total, completed, running int
addLoop:
	for {
		select {
		case newJobMsg := <-s.addJobCh:
			receivedJob = true
			js := newJobState(newJobMsg.job, newJobMsg.saga)
			s.inProgressJobs = append(s.inProgressJobs, js)

			req := newJobMsg.job.Def.Requestor
			if _, ok := s.requestorMap[req]; !ok {
				s.requestorMap[req] = []*jobState{}
			}
			s.requestorMap[req] = append(s.requestorMap[req], js)

			log.Infof("Created new Job: %s, requestor: %s, with %d tasks.",
				newJobMsg.job.Id, req, len(newJobMsg.job.Def.Tasks))
		default:
			break addLoop
		}
	}

	if receivedJob {
		for _, job := range s.inProgressJobs {
			total += len(job.Tasks)
			completed += job.TasksCompleted
			running += job.TasksRunning
		}
		log.Infof("After adding jobs: tasks unscheduled: %d, running: %d, completed: %d, total: %d",
			total-completed-running, running, completed, total)
	}
}

// Helpers, assumes that jobId is present given a consistent scheduler state.
func (s *statefulScheduler) deleteJob(jobId string) {
	var requestor string
	for i, job := range s.inProgressJobs {
		if job.Job.Id == jobId {
			requestor = job.Job.Def.Requestor
			s.inProgressJobs = append(s.inProgressJobs[:i], s.inProgressJobs[i+1:]...)
		}
	}
	jobs := s.requestorMap[requestor]
	for i, job := range jobs {
		if job.Job.Id == jobId {
			s.requestorMap[requestor] = append(jobs[:i], jobs[i+1:]...)
		}
	}
	if len(s.requestorMap[requestor]) == 0 {
		delete(s.requestorMap, requestor)
	}
}

func (s *statefulScheduler) getJob(jobId string) *jobState {
	for _, job := range s.inProgressJobs {
		if job.Job.Id == jobId {
			return job
		}
	}
	return nil
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
						// This job is fully processed remove from InProgressJobs
						s.deleteJob(j.Job.Id)
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
	// Calculate a list of Tasks to Node Assignments & start running all those jobs
	taskAssignments, nodeGroups := getTaskAssignments(s.clusterState, s.inProgressJobs, s.requestorMap)
	if taskAssignments != nil {
		s.clusterState.nodeGroups = nodeGroups
	}
	for _, ta := range taskAssignments {

		// Set up variables for async functions & callback
		jobId := ta.task.JobId
		taskId := ta.task.TaskId
		taskDef := ta.task.Def
		jobState := s.getJob(jobId)
		sa := jobState.Saga
		nodeId := ta.node.Id()
		rs := s.runnerFactory(ta.node)

		preventRetries := bool(ta.task.NumTimesTried >= s.maxRetriesPerTask)

		// This task is co-opting the node for some other running task, abort that task.
		if ta.running != nil {
			// Send the signal to abort whatever is currently running on the node we're about to co-opt.
			rt := ta.running
			endSagaTask := false
			rt.Runner.abortCh <- endSagaTask
			msg := fmt.Sprintf("jobId:%s taskId:%s Preempted by jobId:%s taskId:%s", rt.JobId, rt.TaskId, jobId, taskId)
			log.Infof(msg)
			// Update jobState and clusterState here instead of in the async handler below.
			s.getJob(rt.JobId).errorRunningTask(rt.TaskId, errors.New(msg))
			s.clusterState.taskCompleted(nodeId, rt.TaskId, false)
		}

		// Mark Task as Started
		s.clusterState.taskScheduled(nodeId, taskId, taskDef.SnapshotID)
		log.Infof("job:%s, task:%s, scheduled on node:%s\n", jobId, taskId, nodeId)
		jobState.taskStarted(taskId)

		run := &taskRunner{
			saga:   sa,
			runner: rs,
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

			abortCh: make(chan interface{}),
		}
		ta.task.Runner = run

		s.asyncRunner.RunAsync(
			run.run,
			func(err error) {
				flaky := false
				if jobState.getTask(taskId).Status != sched.InProgress {
					log.Info("Task preempted, skipping asyncRun cleanup of node:", nodeId, ", job:", jobId, ", task:", taskId)
					return
				}
				if err != nil {
					// Get the type of error. Currently we only care to distinguish runner (ex: thrift) errors to mark flaky nodes.
					taskErr := err.(*taskError)
					flaky = (taskErr.runnerErr != nil)

					msg := "Error running job (will be retried):"
					if taskErr.resultErr != nil && taskErr.st.State == runner.COMPLETE {
						msg = "Error running job (quitting, tasks that run to completion are not retried):"
						err = nil
					} else if preventRetries {
						msg = fmt.Sprintf("Error running job (quitting, hit max retries of %d):", s.maxRetriesPerTask)
						err = nil
					} else {
						jobState.errorRunningTask(taskId, err)
					}
					log.Info(msg, jobId, ", task:", taskId, " err:", taskErr, " cmd:", taskDef.Argv)

					// If the task completed succesfully but sagalog failed, start a goroutine to retry until it succeeds.
					if taskErr.sagaErr != nil && taskErr.runnerErr == nil && taskErr.resultErr == nil {
						log.Info(msg, jobId, ", task:", taskId, " -> starting goroutine to handle failed saga.EndTask. ")
						go func() {
							for err := errors.New(""); err != nil; err = run.logTaskStatus(&taskErr.st, saga.EndTask) {
								time.Sleep(time.Second)
							}
							log.Info(msg, jobId, ", task:", taskId, " -> finished goroutine to handle failed saga.EndTask. ")
						}()
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
