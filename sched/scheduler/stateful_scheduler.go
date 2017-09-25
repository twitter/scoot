package scheduler

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	uuid "github.com/nu7hatch/gouuid"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/async"
	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/log/tags"
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
// These are reasonable defaults for a small cluster of around a couple dozen nodes.
// FIXME(jschiller): overwrite SoftMaxSchedulableTasks at runtime if peak load is higher.

// Nothing should run forever by default, use this timeout as a fallback.
const DefaultDefaultTaskTimeout = 30 * time.Minute

// Allow extra time when waiting for a task response.
// This includes network time and the time to upload logs to bundlestore.
const DefaultTaskTimeoutOverhead = 15 * time.Second

// Number of different requestors that can run jobs at any given time.
const DefaultMaxRequestors = 10

// Number of jobs any single requestor can have (to prevent spamming, not for scheduler fairness).
const DefaultMaxJobsPerRequestor = 100

// A reasonable maximum number of tasks we'd expect to queue.
const DefaultSoftMaxSchedulableTasks = 1000

// Increase the NodeScaleFactor by a percentage defined by 1 + (Priority * NodeScaleAdjustment)
var NodeScaleAdjustment = 1.0

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
	SoftMaxSchedulableTasks int
}

// Used to calculate how many tasks a job can run without adversely affecting other jobs.
// We account for priority by increasing the scale factor by an appropriate percentage.
//  ex: p=0:scale*=1, p=1:scale*=1.5, p=2:scale*=2
func (s *SchedulerConfig) GetNodeScaleFactor(numNodes int, p sched.Priority) float32 {
	sf := float32(numNodes) / float32(s.SoftMaxSchedulableTasks)
	return sf * (1 + (float32(p) * float32(NodeScaleAdjustment)))
}

// Used to keep a running average of duration for a specific task.
type averageDuration struct {
	count    int64
	duration time.Duration
}

func (ad averageDuration) update(d time.Duration) {
	ad.count++
	ad.duration = ad.duration + time.Duration(int64(d-ad.duration)/ad.count)
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
	inProgressJobs []*jobState                // ordered list of inprogress jobId to job.
	requestorMap   map[string][]*jobState     // map of requestor to all its jobs. Default requestor="" is ok.
	taskDurations  map[string]averageDuration // map of taskId to averageDuration (note: we unconditionally dereference this).

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
				log.WithFields(
					log.Fields{
						"node": node,
						"err":  svc.Error,
					}).Info("received service err during init of new node")
				return false, 0
			}
			return false, config.ReadyFnBackoff
		}
		for _, s := range st {
			log.WithFields(
				log.Fields{
					"node":       node,
					"runID":      s.RunID,
					"state":      s.State,
					"stdout":     s.StdoutRef,
					"stderr":     s.StderrRef,
					"snapshotID": s.SnapshotID,
					"exitCode":   s.ExitCode,
					"error":      s.Error,
					"jobID":      s.JobID,
					"taskID":     s.TaskID,
					"tag":        s.Tag,
				}).Info("Aborting existing run on new node")
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
	if config.SoftMaxSchedulableTasks == 0 {
		config.SoftMaxSchedulableTasks = DefaultSoftMaxSchedulableTasks
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
		taskDurations:  make(map[string]averageDuration),
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
	log.WithFields(
		log.Fields{
			"requestor": jobDef.Requestor,
			"tag":       jobDef.Tag,
			"basis":     jobDef.Basis,
			"priority":  jobDef.Priority,
			"numTasks":  len(jobDef.Tasks),
		}).Info("New job request")

	checkResultCh := make(chan error, 1)
	s.checkJobCh <- jobCheckMsg{
		jobDef:   &jobDef,
		resultCh: checkResultCh,
	}
	err := <-checkResultCh
	if err != nil {
		log.WithFields(
			log.Fields{
				"jobDef":    jobDef,
				"requestor": jobDef.Requestor,
				"tag":       jobDef.Tag,
				"basis":     jobDef.Basis,
				"priority":  jobDef.Priority,
				"err":       err,
			}).Error("Rejected job request")
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
		log.WithFields(
			log.Fields{
				"jobDef":    jobDef,
				"requestor": jobDef.Requestor,
				"tag":       jobDef.Tag,
				"basis":     jobDef.Basis,
				"priority":  jobDef.Priority,
				"err":       err,
			}).Error("Failed to serialize job request")
		return "", err
	}

	// Log StartSaga Message
	sagaObj, err := s.sagaCoord.MakeSaga(job.Id, asBytes)
	if err != nil {
		log.WithFields(
			log.Fields{
				"jobDef": jobDef,
				"err":    err,
				"tag":    jobDef.Tag,
			}).Error("Failed to create saga for job request")
		return "", err
	}
	log.WithFields(
		log.Fields{
			"requestor": jobDef.Requestor,
			"tag":       jobDef.Tag,
			"basis":     jobDef.Basis,
			"priority":  jobDef.Priority,
			"numTasks":  len(jobDef.Tasks),
		}).Info("Queueing job request")
	s.stat.Counter(stats.SchedJobsCounter).Inc(1)
	s.addJobCh <- jobAddedMsg{
		job:  job,
		saga: sagaObj,
	}

	return job.Id, nil
}

// generates a jobId using a random uuid
func generateJobId() string {
	// uuid.NewV4() should never actually return an error. The code uses
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
					// Not an error since we can consider priority to be a suggestion which we'll handle contextually.
					for _, js := range s.requestorMap[checkJobMsg.jobDef.Requestor] {
						if js.Job.Def.Tag == checkJobMsg.jobDef.Tag &&
							js.Job.Def.Basis != checkJobMsg.jobDef.Basis &&
							js.Job.Def.Priority != checkJobMsg.jobDef.Priority {
							m := checkJobMsg
							log.WithFields(
								log.Fields{
									"requestor": m.jobDef.Requestor,
									"tag":       m.jobDef.Tag,
									"basis":     m.jobDef.Basis,
									"priority":  m.jobDef.Priority,
									"numTasks":  len(m.jobDef.Tasks),
								}).Info("Overriding job priority to match previous requestor/tag priority")
						}
					}
				} else if checkJobMsg.jobDef.Priority > sched.P1 {
					// Priorities greater than 1 are disabled in job_state.go.
					jd := checkJobMsg.jobDef
					log.Infof("Overriding job priority %d to respect max priority of 1 (higher priority is untested and disabled)"+
						"Requestor:%s, Tag:%s, Basis:%s, Priority:%d, numTasks: %d",
						jd.Priority, jd.Requestor, jd.Tag, jd.Basis, jd.Priority, len(jd.Tasks))
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
			js := newJobState(newJobMsg.job, newJobMsg.saga, s.taskDurations)
			s.inProgressJobs = append(s.inProgressJobs, js)

			// TODO(jschiller): associate related tasks, i.e. task retries that decorate the taskId?
			sort.Sort(sort.Reverse(taskStatesByDuration(js.Tasks)))

			req := newJobMsg.job.Def.Requestor
			if _, ok := s.requestorMap[req]; !ok {
				s.requestorMap[req] = []*jobState{}
			}
			s.requestorMap[req] = append(s.requestorMap[req], js)
			log.WithFields(
				log.Fields{
					"jobID":    newJobMsg.job.Id,
					"req":      req,
					"numTasks": len(newJobMsg.job.Def.Tasks),
					"tag":      newJobMsg.job.Def.Tag,
				}).Info("Created new job")
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
		log.WithFields(
			log.Fields{
				"unscheduledTasks": total - completed - running,
				"runningTasks":     running,
				"completedTasks":   completed,
				"totalTasks":       total,
			}).Info("Added jobs")
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
						log.WithFields(
							log.Fields{
								"jobID": j.Job.Id,
								"tag":   j.Job.Def.Tag,
							}).Info("Job completed and logged")
						// This job is fully processed remove from InProgressJobs
						s.deleteJob(j.Job.Id)
					} else {
						// set the jobState flag to false, will retry logging
						// EndSaga message on next scheduler loop
						j.EndingSaga = false
						s.stat.Counter(stats.SchedRetriedEndSagaCounter).Inc(1) // TODO errata metric - remove if unused
						log.WithFields(
							log.Fields{
								"jobID": j.Job.Id,
								"err":   err,
								"tag":   j.Job.Def.Tag,
							}).Info("Job completed but failed to log")
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
		jobID := task.JobId
		taskID := task.TaskId
		tag := s.getJob(jobID).Job.Def.Tag
		taskDef := task.Def
		taskDef.JobID = jobID
		taskDef.Tag = tag
		jobState := s.getJob(jobID)
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
			msg := fmt.Sprintf("jobId:%s taskId:%s Preempted by jobId:%s taskId:%s", rt.JobId, rt.TaskId, jobID, taskID)
			log.Info(msg)
			// Update jobState and clusterState here instead of in the async handler below.
			s.getJob(rt.JobId).errorRunningTask(rt.TaskId, errors.New(msg), preempted)
			s.clusterState.taskCompleted(nodeSt.node.Id(), flaky)
		}

		// Mark Task as Started in the cluster
		s.clusterState.taskScheduled(nodeSt.node.Id(), jobID, taskID, taskDef.SnapshotID)
		log.WithFields(
			log.Fields{
				"jobID":  jobID,
				"taskID": taskID,
				"node":   nodeSt.node,
				"tag":    tag,
			}).Info("Task scheduled")

		tRunner := &taskRunner{
			saga:   sa,
			runner: rs,
			stat:   s.stat,

			defaultTaskTimeout:    s.config.DefaultTaskTimeout,
			taskTimeoutOverhead:   s.config.TaskTimeoutOverhead,
			runnerRetryTimeout:    s.config.RunnerRetryTimeout,
			runnerRetryInterval:   s.config.RunnerRetryInterval,
			markCompleteOnFailure: preventRetries,

			LogTags: tags.LogTags{
				JobID:  jobID,
				TaskID: taskID,
				Tag:    tag,
			},

			task:   taskDef,
			nodeSt: nodeSt,

			abortCh:      make(chan bool, 1),
			queryAbortCh: make(chan interface{}, 1),

			startTime: time.Now(),
		}

		// mark the task as started in the jobState and record its taskRunner
		jobState.taskStarted(taskID, tRunner)

		s.asyncRunner.RunAsync(
			tRunner.run,
			func(err error) {
				defer rs.Release()
				// Update the average duration for this task so, for new jobs, we can schedule the likely long running tasks first.
				if err == nil || err.(*taskError).st.State == runner.TIMEDOUT ||
					(err.(*taskError).st.State == runner.COMPLETE && err.(*taskError).st.ExitCode == 0) {
					s.taskDurations[taskID].update(time.Now().Sub(tRunner.startTime))
				}

				// Tasks from the same job or related jobs will never preempt each other,
				//  so we only need to check jobId to determine preemption.
				if nodeSt.runningJob != jobID {
					log.WithFields(
						log.Fields{
							"node":        nodeSt.node,
							"jobID":       jobID,
							"taskID":      taskID,
							"runningJob":  nodeSt.runningJob,
							"runningTask": nodeSt.runningTask,
							"tag":         tag,
						}).Info("Task preempted")
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
							jobState.errorRunningTask(taskID, err, preempted)
						}
					}
					log.WithFields(
						log.Fields{
							"jobId":  jobID,
							"taskId": taskID,
							"err":    taskErr,
							"cmd":    strings.Join(taskDef.Argv, " "),
							"tag":    tag,
						}).Info(msg)

					// If the task completed succesfully but sagalog failed, start a goroutine to retry until it succeeds.
					if taskErr.sagaErr != nil && taskErr.st.RunID != "" && taskErr.runnerErr == nil && taskErr.resultErr == nil {
						log.WithFields(
							log.Fields{
								"jobId":  jobID,
								"taskId": taskID,
							}).Info(msg, " -> starting goroutine to handle failed saga.EndTask. ")
						//TODO -this may results in closed channel panic due to sending endSaga to sagalog (below) before endTask
						go func() {
							for err := errors.New(""); err != nil; err = tRunner.logTaskStatus(&taskErr.st, saga.EndTask) {
								time.Sleep(time.Second)
							}
							log.WithFields(
								log.Fields{
									"jobId":  jobID,
									"taskId": taskID,
									"tag":    tag,
								}).Info(msg, " -> finished goroutine to handle failed saga.EndTask. ")
						}()
					}
				}
				if err == nil || aborted {
					log.WithFields(
						log.Fields{
							"jobId":   jobID,
							"taskId":  taskID,
							"command": strings.Join(taskDef.Argv, " "),
							"tag":     tag,
						}).Info("Ending task.")
					jobState.taskCompleted(taskID, true)
				}

				// update cluster state that this node is now free and if we consider the runner to be flaky.
				log.WithFields(
					log.Fields{
						"jobId":  jobID,
						"taskId": taskID,
						"node":   nodeSt.node,
						"flaky":  flaky,
						"tag":    tag,
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
				log.WithFields(
					log.Fields{
						"jobId":     jobID,
						"running":   jobState.TasksRunning,
						"completed": jobState.TasksCompleted,
						"total":     len(jobState.Tasks),
						"isdone":    jobState.TasksCompleted == len(jobState.Tasks),
						"tag":       tag,
					}).Info()
				log.WithFields(
					log.Fields{
						"running":   running,
						"completed": completed,
						"total":     total,
						"alldone":   completed == total,
						"tag":       tag,
					}).Info("Jobs task summary")
			})
	}
}

/*
Put the kill request on channel that is processed by the main
scheduler loop, and wait for the response
*/
func (s *statefulScheduler) KillJob(jobID string) error {

	log.WithFields(
		log.Fields{
			"jobID": jobID,
		}).Info("KillJob requested")
	responseCh := make(chan error, 1)
	req := jobKillRequest{jobId: jobID, responseCh: responseCh}
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
				st := runner.AbortStatus("", tags.LogTags{JobID: jobState.Job.Id, TaskID: task.TaskId})
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
		log.WithFields(
			log.Fields{
				"jobID":           req.jobId,
				"tasksNotStarted": notStarted,
				"tasksInProgress": inProgress,
				"tag":             s.getJob(req.jobId).Job.Def.Tag,
			}).Info("killJobs handler")

		req.responseCh <- nil
	}
}
