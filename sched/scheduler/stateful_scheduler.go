package scheduler

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	uuid "github.com/nu7hatch/gouuid"

	"github.com/twitter/scoot/async"
	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/workerapi"
)

// Used to get proper logging from tests...
func init() {
	if loglevel := os.Getenv("SCOOT_LOGLEVEL"); loglevel != "" {
		level, err := log.ParseLevel(loglevel)
		if err != nil {
			log.Error(err)
			return
		}
		log.SetLevel(level)
		log.AddHook(hooks.NewContextHook())
	}
}

// Provide defaults for config settings that should never be uninitialized/zero.

// Nothing should run forever by default, use this timeout as a fallback.
const DefaultDefaultTaskTimeout = 30 * time.Minute

// Allow extra time when waiting for a task response.
// This includes network time and the time to upload logs to bundlestore.
const DefaultTaskTimeoutOverhead = 15 * time.Second

// Number of different requestors that can run jobs at any given time.
const DefaultMaxRequestors = 100

// Number of jobs any single requestor can have.
const DefaultMaxJobsPerRequestor = 100

// Expected number of nodes. Should roughly correspond with the actual number of healthy nodes.
const DefaultNumConfiguredNodes = 100

// A reasonable maximum number of tasks we'd expect to queue.
const DefaultSoftMaxSchedulableTasks = 10000

// The maximum number of nodes required to run a 'large' job in an acceptable amount of time.
const DefaultLargeJobSoftMaxNodes = 50

// Scheduler Config variables read at initialization
// MaxRetriesPerTask - the number of times to retry a failing task before
//     marking it as completed.
// DebugMode - if true, starts the scheduler up but does not start
//     the update loop.  Instead the loop must be advanced manually
//     by calling step()
// RecoverJobsOnStartup - if true, the scheduler recovers active sagas,
//     from the sagalog, and restarts them.
// DefaultTaskTimeout -
//     default timeout for tasks.
// TaskTimeoutOverhead
//     How long to wait for a response after the task has timed out.
// RunnerRetryTimeout -
//     how long to keep retrying a runner req.
// RunnerRetryInterval -
//     how long to sleep between runner req retries.
// ReadyFnBackoff -
//     how long to wait between runner status queries to determine [init] status.

type SchedulerConfig struct {
	MaxRetriesPerTask       int
	DebugMode               bool
	RecoverJobsOnStartup    bool
	DefaultTaskTimeout      time.Duration
	TaskTimeoutOverhead     time.Duration
	RunnerRetryTimeout      time.Duration
	RunnerRetryInterval     time.Duration
	ReadyFnBackoff          time.Duration
	MaxRequestors           int
	MaxJobsPerRequestor     int
	NumConfiguredNodes      int
	SoftMaxSchedulableTasks int
	LargeJobSoftMaxNodes    int
}

// Used to calculate how many tasks a job can run without adversely affecting other jobs.
func (s *SchedulerConfig) GetNodeScaleFactor() float32 {
	return float32(s.NumConfiguredNodes) / float32(s.SoftMaxSchedulableTasks)
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
	config        *SchedulerConfig
	sagaCoord     saga.SagaCoordinator
	runnerFactory RunnerFactory
	asyncRunner   async.Runner
	checkJobCh    chan jobCheckMsg
	addJobCh      chan jobAddedMsg
	killJobCh     chan jobKillRequest

	// Scheduler State
	clusterState   *clusterState
	inProgressJobs []*jobState            // ordered list of inprogress jobId to job.
	requestorMap   map[string][]*jobState // map of requestor to all its jobs. Default requestor="" is ok.

	// stats
	stat stats.StatsReceiver
}

// contains jobId to be killed and callback for the result of processing the request
type jobKillRequest struct {
	jobId      string
	responseCh chan error
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

	if config.DefaultTaskTimeout == 0 {
		config.DefaultTaskTimeout = DefaultDefaultTaskTimeout
	}
	if config.TaskTimeoutOverhead == 0 {
		config.TaskTimeoutOverhead = DefaultTaskTimeoutOverhead
	}
	if config.MaxRequestors == 0 {
		config.MaxRequestors = DefaultMaxRequestors
	}
	if config.MaxJobsPerRequestor == 0 {
		config.MaxJobsPerRequestor = DefaultMaxJobsPerRequestor
	}
	if config.NumConfiguredNodes == 0 {
		config.NumConfiguredNodes = DefaultNumConfiguredNodes
	}
	if config.SoftMaxSchedulableTasks == 0 {
		config.SoftMaxSchedulableTasks = DefaultSoftMaxSchedulableTasks
	}
	if config.LargeJobSoftMaxNodes == 0 {
		config.LargeJobSoftMaxNodes = DefaultLargeJobSoftMaxNodes
	}

	sched := &statefulScheduler{
		config:        &config,
		sagaCoord:     sc,
		runnerFactory: rf,
		asyncRunner:   async.NewRunner(),
		checkJobCh:    make(chan jobCheckMsg, 1),
		addJobCh:      make(chan jobAddedMsg, 1),
		killJobCh:     make(chan jobKillRequest, 1), // TODO - what should this value be?

		clusterState:   newClusterState(initialCluster, clusterUpdates, nodeReadyFn, stat),
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
	jobDef   *sched.JobDefinition
	resultCh chan error
}

type jobAddedMsg struct {
	job  *sched.Job
	saga *saga.Saga
}

func (s *statefulScheduler) ScheduleJob(jobDef sched.JobDefinition) (string, error) {
	defer s.stat.Latency(stats.SchedJobLatency_ms).Time().Stop() // TODO errata metric - remove if unused
	s.stat.Counter(stats.SchedJobRequestsCounter).Inc(1)         // TODO errata metric - remove if unused
	log.Infof("Job request: Requestor:%s, Tag:%s, Basis:%s, Priority:%d, numTasks: %d",
		jobDef.Requestor, jobDef.Tag, jobDef.Basis, jobDef.Priority, len(jobDef.Tasks))

	checkResultCh := make(chan error, 1)
	s.checkJobCh <- jobCheckMsg{
		jobDef:   &jobDef,
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

	log.Infof("Queueing job request: Requestor:%s, Tag:%s, Basis:%s, Priority:%d, numTasks: %d",
		jobDef.Requestor, jobDef.Tag, jobDef.Basis, jobDef.Priority, len(jobDef.Tasks))
	s.stat.Counter(stats.SchedJobsCounter).Inc(1)
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
// we are not putting any logic other than looping in this method so unit tests can verify
// behavior by controlling calls to step() below
func (s *statefulScheduler) loop() {
	for {
		s.step()
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
	s.killJobs()
	s.scheduleTasks()

	remaining := 0
	waitingToStart := 0
	for _, job := range s.inProgressJobs {
		remaining += (len(job.Tasks) - job.TasksCompleted)
		if job.TasksCompleted+job.TasksRunning == 0 {
			waitingToStart += 1
		}
	}
	s.stat.Gauge(stats.SchedAcceptedJobsGauge).Update(int64(len(s.inProgressJobs)))
	s.stat.Gauge(stats.SchedWaitingJobsGauge).Update(int64(waitingToStart))
	s.stat.Gauge(stats.SchedInProgressTasksGauge).Update(int64(remaining))
	s.stat.Gauge(stats.SchedNumRunningTasksGauge).Update(int64(s.asyncRunner.NumRunning()))
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
			var err error
			if jobs, ok := s.requestorMap[checkJobMsg.jobDef.Requestor]; !ok && len(s.requestorMap) >= s.config.MaxRequestors {
				err = fmt.Errorf("Exceeds max number of requestors: %s (%d)", checkJobMsg.jobDef.Requestor, s.config.MaxRequestors)
			} else if len(jobs) >= s.config.MaxJobsPerRequestor {
				err = fmt.Errorf("Exceeds max jobs per requestor: %s (%d)", checkJobMsg.jobDef.Requestor, s.config.MaxJobsPerRequestor)
			} else if checkJobMsg.jobDef.Priority < sched.P0 || checkJobMsg.jobDef.Priority > sched.P3 {
				err = fmt.Errorf("Invalid priority %d, must be between 0-3 inclusive", checkJobMsg.jobDef.Priority)
			} else {
				seenTasks := map[string]bool{}
				for _, t := range checkJobMsg.jobDef.Tasks {
					if _, ok := seenTasks[t.TaskID]; ok {
						err = fmt.Errorf("Invalid dup taskID %s", t.TaskID)
						break
					}
					seenTasks[t.TaskID] = true
				}
				if _, ok := s.requestorMap[checkJobMsg.jobDef.Requestor]; ok && err == nil {
					// If we have an existing job with this requestor/tag combination, make sure we use its priority level.
					// Not an error since we can consider priority to be a suggestion which we'll choose to ignore.
					for _, js := range s.requestorMap[checkJobMsg.jobDef.Requestor] {
						if js.Job.Def.Tag == checkJobMsg.jobDef.Tag &&
							js.Job.Def.Basis != checkJobMsg.jobDef.Basis &&
							js.Job.Def.Priority != checkJobMsg.jobDef.Priority {
							m := checkJobMsg
							log.Infof("Overriding job priority %d to match previous requestor/tag priority: "+
								"Requestor:%s, Tag:%s, Basis:%s, Priority:%d, numTasks: %d",
								js.Job.Def.Priority, m.jobDef.Requestor, m.jobDef.Tag, m.jobDef.Basis, m.jobDef.Priority, len(m.jobDef.Tasks))
						}
					}
				}
			}
			checkJobMsg.resultCh <- err
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
					//FIXME: seeing panic on closed channel here after killjob().
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
						s.stat.Counter(stats.SchedRetriedEndSagaCounter).Inc(1) // TODO errata metric - remove if unused
						log.Infof("Job completed but failed to log: %v, %v", j.Job.Id, err)
					}
				})
		}
	}
}

// figures out which tasks to schedule next and on which worker and then runs them
func (s *statefulScheduler) scheduleTasks() {
	// Calculate a list of Tasks to Node Assignments & start running all those jobs
	taskAssignments, nodeGroups := getTaskAssignments(s.clusterState, s.inProgressJobs, s.requestorMap, s.config, s.stat)
	if taskAssignments != nil {
		s.clusterState.nodeGroups = nodeGroups
	}
	for _, ta := range taskAssignments {
		// Set up variables for async functions & callback
		task := ta.task
		nodeSt := ta.nodeSt
		jobId := task.JobId
		taskId := task.TaskId
		taskDef := task.Def
		jobState := s.getJob(jobId)
		sa := jobState.Saga
		rs := s.runnerFactory(nodeSt.node)

		preventRetries := bool(task.NumTimesTried >= s.config.MaxRetriesPerTask)

		// This task is co-opting the node for some other running task, abort that task.
		if ta.running != nil {
			// Send the signal to abort whatever is currently running on the node we're about to co-opt.
			rt := ta.running
			endSagaTask := false
			flaky := false
			preempted := true
			rt.TaskRunner.Abort(endSagaTask)
			msg := fmt.Sprintf("jobId:%s taskId:%s Preempted by jobId:%s taskId:%s", rt.JobId, rt.TaskId, jobId, taskId)
			log.Infof(msg)
			// Update jobState and clusterState here instead of in the async handler below.
			s.getJob(rt.JobId).errorRunningTask(rt.TaskId, errors.New(msg), preempted)
			s.clusterState.taskCompleted(nodeSt.node.Id(), flaky)
		}

		// Mark Task as Started in the cluster
		s.clusterState.taskScheduled(nodeSt.node.Id(), jobId, taskId, taskDef.SnapshotID)
		log.Infof("jobId:%s, taskId:%s, scheduled on node:%s\n", jobId, taskId, nodeSt.node)

		tRunner := &taskRunner{
			saga:   sa,
			runner: rs,
			stat:   s.stat,

			defaultTaskTimeout:    s.config.DefaultTaskTimeout,
			taskTimeoutOverhead:   s.config.TaskTimeoutOverhead,
			runnerRetryTimeout:    s.config.RunnerRetryTimeout,
			runnerRetryInterval:   s.config.RunnerRetryInterval,
			markCompleteOnFailure: preventRetries,

			jobId:  jobId,
			taskId: taskId,
			task:   taskDef,
			nodeSt: nodeSt,

			abortCh:      make(chan bool, 1),
			queryAbortCh: make(chan interface{}, 1),
		}

		// mark the task as started in the jobState and record its taskRunner
		jobState.taskStarted(taskId, tRunner)

		s.asyncRunner.RunAsync(
			tRunner.run,
			func(err error) {
				defer rs.Release()
				// Tasks from the same job or related jobs will never preempt each other,
				//  so we only need to check jobId to determine preemption.
				if nodeSt.runningJob != jobId {
					log.Infof("Task preempted on node %s. jobId:%s taskId:%s --> jobId:%s taskId:%s. Skipping asyncRun cleanup",
						nodeSt.node, jobId, taskId, nodeSt.runningJob, nodeSt.runningTask)
					return
				}

				flaky := false
				preempted := false
				aborted := (err != nil && err.(*taskError).st.State == runner.ABORTED)
				if err != nil {
					// Get the type of error. Currently we only care to distinguish runner (ex: thrift) errors to mark flaky nodes.
					taskErr := err.(*taskError)
					flaky = (taskErr.runnerErr != nil)

					msg := "Error running job (will be retried):"
					if aborted {
						msg = "Error running task, but job kill request received, (will not retry):"
						err = nil
					} else {
						if preventRetries {
							msg = fmt.Sprintf("Error running task (quitting, hit max retries of %d):", s.config.MaxRetriesPerTask)
							err = nil
						} else {
							jobState.errorRunningTask(taskId, err, preempted)
						}
					}
					log.WithFields(log.Fields{
						"jobId":  jobId,
						"taskId": taskId,
						"err":    taskErr,
						"cmd":    strings.Join(taskDef.Argv, " "),
					}).Info(msg)

					// If the task completed succesfully but sagalog failed, start a goroutine to retry until it succeeds.
					if taskErr.sagaErr != nil && taskErr.st.RunID != "" && taskErr.runnerErr == nil && taskErr.resultErr == nil {
						log.WithFields(log.Fields{
							"jobId":  jobId,
							"taskId": taskId,
						}).Info(msg, " -> starting goroutine to handle failed saga.EndTask. ")
						//TODO -this may results in closed channel panic due to sending endSaga to sagalog (below) before endTask
						go func() {
							for err := errors.New(""); err != nil; err = tRunner.logTaskStatus(&taskErr.st, saga.EndTask) {
								time.Sleep(time.Second)
							}
							log.WithFields(log.Fields{
								"jobId":  jobId,
								"taskId": taskId,
							}).Info(msg, " -> finished goroutine to handle failed saga.EndTask. ")
						}()
					}
				}
				if err == nil || aborted {
					log.WithFields(log.Fields{
						"jobId":   jobId,
						"taskId":  taskId,
						"command": strings.Join(taskDef.Argv, " "),
					}).Info("Ending task.")
					jobState.taskCompleted(taskId, true)
				}

				// update cluster state that this node is now free and if we consider the runner to be flaky.
				log.WithFields(log.Fields{
					"jobId":  jobId,
					"taskId": taskId,
					"node":   nodeSt.node,
					"flaky":  flaky,
				}).Info("Freeing node, removed job.")
				s.clusterState.taskCompleted(nodeSt.node.Id(), flaky)

				total := 0
				completed := 0
				running := 0
				for _, job := range s.inProgressJobs {
					total += len(job.Tasks)
					completed += job.TasksCompleted
					running += job.TasksRunning
				}
				log.WithFields(log.Fields{
					"jobId": jobId,
				}).Info(" #running:", jobState.TasksRunning, " #completed:", jobState.TasksCompleted,
					" #total:", len(jobState.Tasks), " isdone:", (jobState.TasksCompleted == len(jobState.Tasks)))
				log.Info("Jobs task summary -> running:", running, " completed:", completed,
					" total:", total, " alldone:", (completed == total))
			})
	}
}

/*
Put the kill request on channel that is processed by the main
scheduler loop, and wait for the response
*/
func (s *statefulScheduler) KillJob(jobId string) error {

	log.WithFields(log.Fields{
		"jobId": jobId,
	}).Info("KillJob requested")
	responseCh := make(chan error, 1)
	req := jobKillRequest{jobId: jobId, responseCh: responseCh}
	s.killJobCh <- req

	return <-req.responseCh

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
			jobState := s.getJob(req.jobId)
			if jobState == nil {
				req.responseCh <- fmt.Errorf("Cannot kill Job Id %s, not found."+
					" (This error can be ignored if kill was a fire-and-forget defensive request)."+
					" The job may be finished, "+
					" the request may still be in the queue to be scheduled, or "+
					" the id may be invalid.  "+
					" Check the job status, verify the id and/or resubmit the kill request after a few moments.",
					req.jobId)
			} else if jobState.JobKilled {
				req.responseCh <- fmt.Errorf("Job Id %s was already killed, request ignored", req.jobId)
			} else {
				jobState.JobKilled = true
				validKillRequests = append(validKillRequests[:], req)
			}
		default:
			haveKillRequest = false
		}
	}

	// kill the jobs with valid ids
	for _, req := range validKillRequests {
		jobState := s.getJob(req.jobId)
		inProgress, notStarted := 0, 0
		for _, task := range jobState.Tasks {
			if task.Status == sched.InProgress {
				task.TaskRunner.Abort(true)
				inProgress++
			} else if task.Status == sched.NotStarted {
				st := runner.AbortStatus("", runner.LogTags{JobID: jobState.Job.Id, TaskID: task.TaskId})
				statusAsBytes, err := workerapi.SerializeProcessStatus(st)
				if err != nil {
					s.stat.Counter(stats.SchedFailedTaskSerializeCounter).Inc(1) // TODO errata metric - remove if unused
				}
				//TODO - is this the correct counter?
				s.stat.Counter(stats.SchedCompletedTaskCounter).Inc(1)
				jobState.Saga.EndTask(task.TaskId, statusAsBytes)
				jobState.taskCompleted(task.TaskId, false)
				notStarted++
			}
		}
		log.WithFields(log.Fields{
			"jobId":           req.jobId,
			"tasksNotStarted": notStarted,
			"tasksInProgress": inProgress,
		}).Info("killJobs handler")

		req.responseCh <- nil
	}
}
