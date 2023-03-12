package server

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	uuid "github.com/nu7hatch/gouuid"
	log "github.com/sirupsen/logrus"

	"github.com/wisechengyi/scoot/async"
	cc "github.com/wisechengyi/scoot/cloud/cluster"
	"github.com/wisechengyi/scoot/common/log/hooks"
	"github.com/wisechengyi/scoot/common/log/tags"
	"github.com/wisechengyi/scoot/common/stats"
	"github.com/wisechengyi/scoot/runner"
	"github.com/wisechengyi/scoot/saga"
	"github.com/wisechengyi/scoot/scheduler/domain"
	worker "github.com/wisechengyi/scoot/worker/domain"
)

const (
	// Clients will check for this string to differentiate between scoot and user initiated actions.
	UserRequestedErrStr = "UserRequested"

	// Provide defaults for config settings that should never be uninitialized/zero.
	// These are reasonable defaults for a small cluster of around a couple dozen nodes.

	// Nothing should run forever by default, use this timeout as a fallback.
	DefaultDefaultTaskTimeout = 30 * time.Minute

	// Allow extra time when waiting for a task response.
	// This includes network time and the time to upload logs to bundlestore.
	DefaultTaskTimeoutOverhead = 15 * time.Second

	// Number of different requestors that can run jobs at any given time.
	DefaultMaxRequestors = 10

	// Number of jobs any single requestor can have (to prevent spamming, not for scheduler fairness).
	DefaultMaxJobsPerRequestor = 100

	// Set the maximum number of tasks we'd expect to queue to a nonzero value (it'll be overridden later).
	DefaultSoftMaxSchedulableTasks = 1

	// Threshold for jobs considered long running
	LongJobDuration = 4 * time.Hour

	// How often Scheduler step is called in loop
	TickRate = 250 * time.Millisecond

	// The max job priority we respect (higher priority is untested and disabled)
	MaxPriority = domain.P2

	// Max number of requestors to track tag history, and max number of tags per requestor to track
	DefaultMaxRequestorHistories = 1000000
	DefaultMaxHistoryTags        = 100

	// Max number of task IDs to track durations for
	DefaultMaxTaskDurations = 1000000
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
	} else {
		// setting Error level to avoid Travis test failure due to log too long
		log.SetLevel(log.ErrorLevel)
	}
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// nopDurationKeyExtractor returns an unchanged key.
func nopDurationKeyExtractor(key string) string {
	return key
}

// SchedulerConfiguration variables read at initialization
// MaxRetriesPerTask - the number of times to retry a failing task before
//
//	marking it as completed.
//
// DebugMode - if true, starts the scheduler up but does not start
//
//	the update loop.  Instead the loop must be advanced manually
//	by calling step()
//
// RecoverJobsOnStartup - if true, the scheduler recovers active sagas,
//
//	from the sagalog, and restarts them.
//
// DefaultTaskTimeout -
//
//	default timeout for tasks.
//
// TaskTimeoutOverhead
//
//	How long to wait for a response after the task has timed out.
//
// RunnerRetryTimeout -
//
//	how long to keep retrying a runner req.
//
// RunnerRetryInterval -
//
//	how long to sleep between runner req retries.
//
// ReadyFnBackoff -
//
//	how long to wait between runner status queries to determine [init] status.
//
// TaskThrottle -
//
//		   requestors will try not to schedule jobs that make the scheduler exceed
//	    the TaskThrottle.  Note: Sickle may exceed it with retries.
type SchedulerConfiguration struct {
	MaxRetriesPerTask    int
	DebugMode            bool
	RecoverJobsOnStartup bool
	DefaultTaskTimeout   time.Duration
	TaskTimeoutOverhead  time.Duration
	RunnerRetryTimeout   time.Duration
	RunnerRetryInterval  time.Duration
	ReadyFnBackoff       time.Duration
	MaxRequestors        int
	MaxJobsPerRequestor  int
	TaskThrottle         int
	Admins               []string

	SchedAlgConfig interface{}
	SchedAlg       SchedulingAlgorithm
}

func (sc *SchedulerConfiguration) String() string {
	return fmt.Sprintf("SchedulerConfiguration: MaxRetriesPerTask: %d, DebugMode: %t, RecoverJobsOnStartup: %t, DefaultTaskTimeout: %s, "+
		"TaskTimeoutOverhead: %s, RunnerRetryTimeout: %s, RunnerRetryInterval: %s, MaxRequestors: %d, MaxJobsPerRequestor: %d, TaskThrottle: %d, "+
		"Admins: %v",
		sc.MaxRetriesPerTask, sc.DebugMode, sc.RecoverJobsOnStartup, sc.DefaultTaskTimeout, sc.TaskTimeoutOverhead, sc.RunnerRetryTimeout,
		sc.RunnerRetryInterval, sc.MaxRequestors, sc.MaxJobsPerRequestor, sc.TaskThrottle, sc.Admins)
}

// Used to keep a running average of duration for a specific task.
type averageDuration struct {
	count    int64
	duration time.Duration
}

func (ad *averageDuration) update(d time.Duration) {
	ad.count++
	ad.duration = ad.duration + time.Duration(int64(d-ad.duration)/ad.count)
}

type RunnerFactory func(node cc.Node) runner.Service

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
	config        *SchedulerConfiguration
	sagaCoord     saga.SagaCoordinator
	runnerFactory RunnerFactory
	asyncRunner   async.Runner
	checkJobCh    chan jobCheckMsg
	addJobCh      chan jobAddedMsg
	killJobCh     chan jobKillRequest
	stepTicker    *time.Ticker

	// Scheduler State
	clusterState   *clusterState
	inProgressJobs []*jobState // ordered list (by jobId) of jobs being scheduled.  Note: it might be
	// no tasks have started yet.
	requestorMap     map[string][]*jobState // map of requestor to all its jobs. Default requestor="" is ok.
	requestorHistory *lru.Cache             // cache of join(requestor, basis) to new tags in the order received.
	taskDurations    *lru.Cache

	tasksByJobClassAndStartTimeSec map[taskClassAndStartKey]taskStateByJobIDTaskID // map of tasks by their class and start time

	// stats
	stat stats.StatsReceiver

	persistor      Persistor
	TaskThrottleMu sync.RWMutex // mutex must be here to avoid copying the lock when passing config to scheduler constructor

	// durationKeyExtractorFn - function to extract, from taskID, the key to use for tracking task average durations
	durationKeyExtractorFn func(string) string

	SchedAlg SchedulingAlgorithm
}

func (s *statefulScheduler) String() string {
	return fmt.Sprintf("%s, num nodes: %d, num suspended nodes: %d, maxFlakyDuration: %s, maxLostDuration: %s",
		s.config, len(s.clusterState.nodes), len(s.clusterState.suspendedNodes), s.clusterState.maxFlakyDuration, s.clusterState.maxLostDuration)
}

// contains jobId to be killed and callback for the result of processing the request
type jobKillRequest struct {
	jobId      string
	responseCh chan error
}

// Create a New StatefulScheduler that implements the Scheduler interface
// cc.Cluster - cluster of worker nodes
// saga.SagaCoordinator - the Saga Coordinator to log to and recover from
// RunnerFactory - Function which converts a node to a Runner
// SchedulerConfig - additional configuration settings for the scheduler
// StatsReceiver - stats receiver to log statistics to
// specifying debugMode true, starts the scheduler up but does not start
// the update loop.  Instead the loop must be advanced manually by calling
// step(), intended for debugging and test cases
// If recoverJobsOnStartup is true Active Sagas in the saga log will be recovered
// and rescheduled, otherwise no recovery will be done on startup
func NewStatefulScheduler(
	nodesUpdatesCh chan []cc.NodeUpdate,
	sc saga.SagaCoordinator,
	rf RunnerFactory,
	config SchedulerConfiguration,
	stat stats.StatsReceiver,
	persistor Persistor,
	durationKeyExtractorFn func(string) string) *statefulScheduler {
	nodeReadyFn := func(node cc.Node) (bool, time.Duration) {
		run := rf(node)
		st, svc, err := run.StatusAll()
		if err != nil || !svc.Initialized || !svc.IsHealthy {
			if svc.Error != nil {
				log.WithFields(
					log.Fields{
						"node": node,
						"err":  svc.Error,
					}).Info("received service err")
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
	config.SchedAlgConfig = &LoadBasedAlgConfig{
		stat: stat,
	}

	// create the load base scheduling algorithm
	tasksByClassAndStartMap := map[taskClassAndStartKey]taskStateByJobIDTaskID{}
	sa := NewLoadBasedAlg(config.SchedAlgConfig.(*LoadBasedAlgConfig), tasksByClassAndStartMap)
	sa.setClassLoadPercents(DefaultLoadBasedSchedulerClassPercents)
	sa.setRequestorToClassMap(DefaultRequestorToClassMap)
	sa.setRebalanceMinimumDuration(DefaultMinRebalanceTime)
	config.SchedAlg = sa
	config.SchedAlgConfig = sa.config

	requestorHistory, err := lru.New(DefaultMaxRequestorHistories)
	if err != nil {
		log.Errorf("Failed to create requestorHistory cache: %v", err)
		return nil
	}

	taskDurations, err := lru.New(DefaultMaxTaskDurations)
	if err != nil {
		log.Errorf("Failed to create taskDurations cache: %v", err)
		return nil
	}

	dkef := durationKeyExtractorFn
	if durationKeyExtractorFn == nil {
		dkef = nopDurationKeyExtractor
	}

	sched := &statefulScheduler{
		config:        &config,
		sagaCoord:     sc,
		runnerFactory: rf,
		asyncRunner:   async.NewRunner(),
		checkJobCh:    make(chan jobCheckMsg, 1),
		addJobCh:      make(chan jobAddedMsg, 1),
		killJobCh:     make(chan jobKillRequest, 1), // TODO - what should this value be?
		stepTicker:    time.NewTicker(TickRate),

		clusterState:     newClusterState(nodesUpdatesCh, nodeReadyFn, stat),
		inProgressJobs:   make([]*jobState, 0),
		requestorMap:     make(map[string][]*jobState),
		requestorHistory: requestorHistory,
		taskDurations:    taskDurations,
		stat:             stat,

		tasksByJobClassAndStartTimeSec: tasksByClassAndStartMap,
		persistor:                      persistor,
		durationKeyExtractorFn:         dkef,
	}

	sched.setThrottle(-1)

	if sched.persistor == nil {
		log.Info("setting persistor is nil, settings will reset to default values on scheduler reset")
	}
	sched.loadSettings()

	log.Info(sched)

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
	jobDef   *domain.JobDefinition
	resultCh chan error
}

type jobAddedMsg struct {
	job  *domain.Job
	saga *saga.Saga
}

type requestorCounts struct {
	numJobsRunning         int
	numJobsWaitingToStart  int
	numTasksRunning        int
	numTasksWaitingToStart int
	numRemainingTasks      int // not completed tasks
}

/*
validate the job request. If the job passes validation, the job's tasks are queued for processing as
per the task scheduling algorithm and an id for the job is returned, otherwise the error message is returned.
*/
func (s *statefulScheduler) ScheduleJob(jobDef domain.JobDefinition) (string, error) {
	/*
		Put the job request and a callback channel on the check job channel.  Wait for the
		scheduling thread to pick up the request from the check job channel, verify that it
		can handle the request and return either nil or an error on the callback channel.

		If no error is found, generate an id for the job, start a saga for the job and add the
		job to the add job channel.

		 Return either the error message or job id to the caller.
	*/
	defer s.stat.Latency(stats.SchedJobLatency_ms).Time().Stop()
	s.stat.Counter(stats.SchedJobRequestsCounter).Inc(1)
	log.WithFields(
		log.Fields{
			"requestor": jobDef.Requestor,
			"jobType":   jobDef.JobType,
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
				"jobType":   jobDef.JobType,
				"tag":       jobDef.Tag,
				"basis":     jobDef.Basis,
				"priority":  jobDef.Priority,
				"err":       err,
			}).Error("Rejected job request")
		return "", err
	}

	job := &domain.Job{
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
				"jobType":   jobDef.JobType,
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
				"jobDef":    jobDef,
				"err":       err,
				"requestor": jobDef.Requestor,
				"jobType":   jobDef.JobType,
				"tag":       jobDef.Tag,
			}).Error("Failed to create saga for job request")
		return "", err
	}
	log.WithFields(
		log.Fields{
			"requestor": jobDef.Requestor,
			"jobType":   jobDef.JobType,
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

// run the scheduler loop indefinitely in its own thread.
// we are not putting any logic other than looping in this method so unit tests can verify
// behavior by controlling calls to step() below
func (s *statefulScheduler) loop() {
	for {
		s.step()

		// Wait until our TickRate has elapsed or we have a pending action.
		// Detect pending action by monitoring statefulScheduler's job channels.
		// Since "detect" means we pulled off of a channel, put it back,
		// asynchronously in case the channel is blocked/full (it will be drained next step())
		select {
		case msg := <-s.checkJobCh:
			go func() {
				s.checkJobCh <- msg
			}()
		case msg := <-s.addJobCh:
			go func() {
				s.addJobCh <- msg
			}()
		case msg := <-s.killJobCh:
			go func() {
				s.killJobCh <- msg
			}()
		case <-s.stepTicker.C:
		}
	}
}

// run one loop iteration
func (s *statefulScheduler) step() {
	defer s.stat.Latency(stats.SchedStepLatency_ms).Time().Stop()

	// update scheduler state with messages received since last loop
	// nodes added or removed to cluster, new jobs scheduled,
	// async functions completed & invoke callbacks
	s.addJobs()
	s.clusterState.updateCluster()

	procMessagesLatency := s.stat.Latency(stats.SchedProcessMessagesLatency_ms).Time()
	s.asyncRunner.ProcessMessages()
	procMessagesLatency.Stop()

	// TODO: make processUpdates on scheduler state wait until an update
	// has been received
	// instead of just burning CPU and constantly looping while no updates
	// have occurred
	s.checkForCompletedJobs()
	s.killJobs()
	s.scheduleTasks()

	s.updateStats()
}

// update the stats monitoring values:
// number of job requests running or waiting to start
// number of jobs waiting to start
// number of tasks currently running
// total number of waiting or running tasks
//
// for each unique requestor count:
// . number of tasks running
// . number of tasks waiting
// . number of jobs running or waiting to start
func (s *statefulScheduler) updateStats() {
	remainingTasks := 0
	waitingTasks := 0
	runningTasks := 0
	jobsWaitingToStart := 0

	requestorsCounts := make(map[string]*requestorCounts) // map of requestor to job and task stats counts

	// get job and task counts by requestor, and overall jobs stats
	for _, job := range s.inProgressJobs {
		requestor := job.Job.Def.Requestor
		if _, ok := requestorsCounts[requestor]; !ok {
			// first time we've seen this requestor, initialize its map entry
			counts := &requestorCounts{}
			requestorsCounts[requestor] = counts
		}

		running := job.GetNumRunning()
		completed := job.GetNumCompleted()
		remaining := len(job.Tasks) - completed
		waiting := remaining - running
		status := job.getJobStatus()
		// accumulate totals independent of requestors
		remainingTasks += remaining
		waitingTasks += waiting
		runningTasks += running

		// totals by requestor
		if job.TasksCompleted+job.TasksRunning == 0 {
			jobsWaitingToStart += 1
		} else if status == domain.InProgress {
			requestorsCounts[requestor].numJobsRunning++
		}
		requestorsCounts[requestor].numRemainingTasks += remaining
		requestorsCounts[requestor].numTasksRunning += running
		requestorsCounts[requestor].numTasksWaitingToStart += waiting

		if time.Since(job.TimeMarker) > LongJobDuration {
			job.TimeMarker = time.Now()
			log.WithFields(
				log.Fields{
					"requestor":      job.Job.Def.Requestor,
					"jobType":        job.Job.Def.JobType,
					"jobId":          job.Job.Id,
					"tag":            job.Job.Def.Tag,
					"basis":          job.Job.Def.Basis,
					"priority":       job.Job.Def.Priority,
					"numTasks":       len(job.Tasks),
					"tasksRunning":   job.TasksRunning,
					"tasksCompleted": job.TasksCompleted,
					"runTime":        time.Since(job.TimeCreated),
				}).Info("Long-running job")
		}
	}

	// publish the requestor stats
	tasksRunningCheckSum := 0
	tasksWaitingCheckSum := 0
	tasksRemainingCheckSum := 0
	for requestor, counts := range requestorsCounts {
		s.stat.Gauge(fmt.Sprintf("%s_%s", stats.SchedNumRunningJobsGauge, requestor)).Update(int64(
			counts.numJobsRunning))
		s.stat.Gauge(fmt.Sprintf("%s_%s", stats.SchedWaitingJobsGauge, requestor)).Update(int64(
			counts.numJobsWaitingToStart))
		s.stat.Gauge(fmt.Sprintf("%s_%s", stats.SchedNumRunningTasksGauge, requestor)).Update(int64(
			counts.numTasksRunning))
		s.stat.Gauge(fmt.Sprintf("%s_%s", stats.SchedNumWaitingTasksGauge, requestor)).Update(int64(
			counts.numTasksWaitingToStart))
		s.stat.Gauge(fmt.Sprintf("%s_%s", stats.SchedInProgressTasksGauge, requestor)).Update(int64(
			counts.numRemainingTasks))
		tasksRunningCheckSum += counts.numTasksRunning
		tasksWaitingCheckSum += counts.numTasksWaitingToStart
		tasksRemainingCheckSum += counts.numRemainingTasks
	}

	if tasksRunningCheckSum != runningTasks || tasksWaitingCheckSum != waitingTasks || tasksRemainingCheckSum != remainingTasks {
		log.Errorf("stats checksum error\nrunning: expected: %d, got:%d\n waiting: expected: %d, got:%d\nremaining: expected:%d, got:%d",
			runningTasks, tasksRunningCheckSum, waitingTasks, tasksWaitingCheckSum, remainingTasks, tasksRemainingCheckSum)
	}

	// publish the rest of the stats
	s.stat.Gauge(stats.SchedAcceptedJobsGauge).Update(int64(len(s.inProgressJobs)))
	s.stat.Gauge(stats.SchedWaitingJobsGauge).Update(int64(jobsWaitingToStart))
	s.stat.Gauge(stats.SchedInProgressTasksGauge).Update(int64(remainingTasks))
	s.stat.Gauge(stats.SchedNumRunningTasksGauge).Update(int64(runningTasks))
	s.stat.Gauge(stats.SchedNumWaitingTasksGauge).Update(int64(waitingTasks))
	s.stat.Gauge(stats.SchedNumAsyncRunnersGauge).Update(int64(s.asyncRunner.NumRunning())) //TODO remove when done debugging

	// print internal data structure sizes
	var lbs *LoadBasedAlg = s.config.SchedAlg.(*LoadBasedAlg)
	lbsStats := lbs.GetDataStructureSizeStats()
	for k, v := range lbsStats {
		s.stat.Gauge(k).Update(int64(v))
	}
	s.stat.Gauge(stats.SchedTaskStartTimeMapSize).Update(int64(s.getSchedTaskStartTimeMapSize()))
	s.stat.Gauge(stats.SchedInProgressJobsSize).Update(int64(len(s.inProgressJobs)))
	s.stat.Gauge(stats.SchedRequestorMapSize).Update(int64(len(s.requestorMap)))
	s.stat.Gauge(stats.SchedRequestorHistorySize).Update(int64(s.requestorHistory.Len()))
	s.stat.Gauge(stats.SchedTaskDurationsSize).Update(int64(s.taskDurations.Len()))
	s.stat.Gauge(stats.SchedSagasSize).Update(int64(s.sagaCoord.GetNumSagas()))
	s.stat.Gauge(stats.SchedRunnersSize).Update(int64(s.asyncRunner.NumRunning()))
}

// getSchedTaskStartTimeMapSize get the number of running tasks being tracked by tasksByJobClassAndStartTimeSec map
// (should never be larger than the number of workers)
func (s *statefulScheduler) getSchedTaskStartTimeMapSize() int {
	sz := 0
	for _, v := range s.tasksByJobClassAndStartTimeSec {
		sz += len(v)
	}
	return sz
}

func (s *statefulScheduler) getSchedulerTaskCounts() (int, int, int) {
	// get the count of total tasks across all jobs, completed tasks and
	// running tasks
	var total, completed, running int
	for _, job := range s.inProgressJobs {
		total += len(job.Tasks)
		completed += job.TasksCompleted
		running += job.TasksRunning
	}

	return total, completed, running
}

// Checks all new job requests (on the check job channel) that have come in since the last iteration of the step() loop,
// verify the job request: it doesn't exceed the requestor's limits or number of requestors, has a valid priority and
// doesn't duplicate tasks in another new job request.
//
// If the job fails the validation, put an error on the job's callback channel, otherwise put nil on the job's callback
// channel.
func (s *statefulScheduler) checkJobsLoop() {
	defer s.stat.Latency(stats.SchedCheckJobsLoopLatency_ms).Time().Stop()
	for {
		select {
		case checkJobMsg := <-s.checkJobCh:
			var err error
			if jobs, ok := s.requestorMap[checkJobMsg.jobDef.Requestor]; !ok && len(s.requestorMap) >= s.config.MaxRequestors {
				err = fmt.Errorf("exceeds max number of requestors: %s (%d)", checkJobMsg.jobDef.Requestor, s.config.MaxRequestors)
			} else if len(jobs) >= s.config.MaxJobsPerRequestor {
				err = fmt.Errorf("exceeds max jobs per requestor: %s (%d)", checkJobMsg.jobDef.Requestor, s.config.MaxJobsPerRequestor)
			} else if checkJobMsg.jobDef.Priority < domain.P0 || checkJobMsg.jobDef.Priority > domain.P2 {
				err = fmt.Errorf("invalid priority %d, must be between 0-2 inclusive", checkJobMsg.jobDef.Priority)
			} else {
				// Check for duplicate task names
				seenTasks := map[string]bool{}
				for _, t := range checkJobMsg.jobDef.Tasks {
					if _, ok := seenTasks[t.TaskID]; ok {
						err = fmt.Errorf("invalid dup taskID %s", t.TaskID)
						break
					}
					seenTasks[t.TaskID] = true
				}
			}
			if err == nil && checkJobMsg.jobDef.Basis != "" {
				// Check if the given tag is expired for the given requestor & basis.
				rb := checkJobMsg.jobDef.Requestor + checkJobMsg.jobDef.Basis
				rh := getRequestorHistory(s.requestorHistory, rb)
				if stringInSlice(checkJobMsg.jobDef.Tag, rh) &&
					rh[len(rh)-1] != checkJobMsg.jobDef.Tag {
					err = fmt.Errorf("expired tag=%s for basis=%s. Expected either tag=%s or new tag",
						checkJobMsg.jobDef.Tag, checkJobMsg.jobDef.Basis, rh[len(rh)-1])
				} else {
					addOrUpdateRequestorHistory(s.requestorHistory, rb, checkJobMsg.jobDef.Tag)
				}
			}
			checkJobMsg.resultCh <- err
		default:
			return
		}
	}
}

// after all new job requests have been verified, get all jobs that were put in the add job channel since the last
// pass through step() and add them to the inProgress list, order the tasks in the job by descending duration and
// add the job to the requestor map
func (s *statefulScheduler) addJobsLoop() {
	defer s.stat.Latency(stats.SchedAddJobsLoopLatency_ms).Time().Stop()
	var (
		receivedJob bool
		total       int
		completed   int
		running     int
	)
	for {
		select {
		case newJobMsg := <-s.addJobCh:
			receivedJob = true
			lf := log.Fields{
				"jobID":     newJobMsg.job.Id,
				"requestor": newJobMsg.job.Def.Requestor,
				"jobType":   newJobMsg.job.Def.JobType,
				"tag":       newJobMsg.job.Def.Tag,
				"basis":     newJobMsg.job.Def.Basis,
				"priority":  newJobMsg.job.Def.Priority,
				"numTasks":  len(newJobMsg.job.Def.Tasks),
			}
			if _, ok := s.requestorMap[newJobMsg.job.Def.Requestor]; ok {
				for _, js := range s.requestorMap[newJobMsg.job.Def.Requestor] {
					// Kill any prior jobs that share the same requestor and non-empty basis string,
					// but tag should be different so we don't kill the original jobs when doing retries for example.
					// (retries share the same requestor, basis, and tag values).
					if newJobMsg.job.Def.Basis != "" &&
						js.Job.Def.Basis == newJobMsg.job.Def.Basis &&
						js.Job.Def.Tag != newJobMsg.job.Def.Tag {
						log.WithFields(lf).Infof("Matching basis, killing prevJobID=%s", js.Job.Id)
						go s.KillJob(js.Job.Id) // we are in the main thread, this function must be called async.
					}

					// If we have an existing job with this requestor/tag combination, make sure we use its priority level.
					// Not an error since we can consider priority to be a suggestion which we'll handle contextually.
					if js.Job.Def.Tag == newJobMsg.job.Def.Tag &&
						js.Job.Def.Basis != newJobMsg.job.Def.Basis &&
						js.Job.Def.Priority != newJobMsg.job.Def.Priority {
						log.WithFields(lf).Info("Overriding job priority to match previous requestor/tag priority")
						newJobMsg.job.Def.Priority = js.Job.Def.Priority
					}
				}
			} else if newJobMsg.job.Def.Priority > MaxPriority {
				// Priorities greater than 2 are disabled in job_state.go.
				jd := newJobMsg.job.Def
				log.Infof("Overriding job priority %d to respect max priority of %d (higher priority is untested and disabled)"+
					"Requestor:%s, JobType:%s, Tag:%s, Basis:%s, Priority:%d, numTasks: %d",
					jd.Priority, MaxPriority, jd.Requestor, jd.JobType, jd.Tag, jd.Basis, jd.Priority, len(jd.Tasks))
				newJobMsg.job.Def.Priority = MaxPriority
			}

			reqToClassMap, _ := s.GetRequestorToClassMap()
			jc := GetRequestorClass(newJobMsg.job.Def.Requestor, reqToClassMap)
			js := newJobState(newJobMsg.job, jc, newJobMsg.saga, s.taskDurations, s.tasksByJobClassAndStartTimeSec, s.durationKeyExtractorFn)
			s.inProgressJobs = append(s.inProgressJobs, js)

			sort.Sort(sort.Reverse(taskStatesByDuration(js.Tasks)))
			req := newJobMsg.job.Def.Requestor
			if _, ok := s.requestorMap[req]; !ok {
				s.requestorMap[req] = []*jobState{}
			}
			s.requestorMap[req] = append(s.requestorMap[req], js)
			log.WithFields(lf).Info("Created new job")
		default:
			if receivedJob {
				total, completed, running = s.getSchedulerTaskCounts()
				log.WithFields(
					log.Fields{
						"unscheduledTasks": total - completed - running,
						"runningTasks":     running,
						"completedTasks":   completed,
						"totalTasks":       total,
					}).Info("Added jobs")
			}
			return
		}
	}
}

// Checks if any new jobs have been requested since the last loop and adds
// them to the jobs the scheduler is handling
func (s *statefulScheduler) addJobs() {
	defer s.stat.Latency(stats.SchedAddJobsLatency_ms).Time().Stop()
	s.checkJobsLoop()
	s.addJobsLoop()
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
	defer s.stat.Latency(stats.SchedCheckForCompletedLatency_ms).Time().Stop()
	// Check For Completed Jobs & Log EndSaga Message
	for _, jobState := range s.inProgressJobs {
		if jobState.getJobStatus() == domain.Completed && !jobState.EndingSaga {
			// mark job as being completed
			jobState.EndingSaga = true

			// set up variables for async functions for async function & callbacks
			j := jobState

			s.asyncRunner.RunAsync(
				func() error {
					// FIXME: seeing panic on closed channel here after killjob().
					return j.Saga.EndSaga()
				},
				func(err error) {
					if err == nil {
						log.WithFields(
							log.Fields{
								"jobID":     j.Job.Id,
								"requestor": j.Job.Def.Requestor,
								"jobType":   j.Job.Def.JobType,
								"tag":       j.Job.Def.Tag,
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
								"jobID":     j.Job.Id,
								"err":       err,
								"requestor": j.Job.Def.Requestor,
								"jobType":   j.Job.Def.JobType,
								"tag":       j.Job.Def.Tag,
							}).Info("Job completed but failed to log")
					}
				})
		}
	}
}

// figures out which tasks to schedule next and on which worker and then runs them
func (s *statefulScheduler) scheduleTasks() {
	// Calculate a list of Tasks to Node Assignments & start running all those jobs
	// Pass nil config so taskScheduler can determine the most appropriate values itself.
	defer s.stat.Latency(stats.SchedScheduleTasksLatency_ms).Time().Stop()
	taskAssignments := s.getTaskAssignments()
	for _, ta := range taskAssignments {
		// Set up variables for async functions & callback
		task := ta.task
		nodeSt := ta.nodeSt
		jobID := task.JobId
		taskID := task.TaskId
		requestor := s.getJob(jobID).Job.Def.Requestor
		jobType := s.getJob(jobID).Job.Def.JobType
		tag := s.getJob(jobID).Job.Def.Tag
		taskDef := task.Def
		taskDef.JobID = jobID
		taskDef.Tag = tag
		jobState := s.getJob(jobID)
		sa := jobState.Saga
		rs := s.runnerFactory(nodeSt.node)
		durationID := s.durationKeyExtractorFn(taskID)

		preventRetries := bool(task.NumTimesTried >= s.config.MaxRetriesPerTask)

		avgDur := -1
		iface, ok := s.taskDurations.Get(durationID)
		if ok {
			ad, ok := iface.(*averageDuration)
			if !ok {
				log.Errorf("getting task duration, object was not *averageDuration type!  (it is %s)", reflect.TypeOf(ad))
			} else {
				avgDur = int(ad.duration)
			}
		}

		log.WithFields(
			log.Fields{
				"jobID":       jobID,
				"taskID":      taskID,
				"node":        nodeSt.node,
				"requestor":   requestor,
				"jobType":     jobType,
				"tag":         tag,
				"taskDef":     taskDef,
				"durationID":  durationID,
				"avgDuration": fmt.Sprintf("%d (sec)", avgDur),
			}).Info("Starting taskRunner")

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

			abortCh:      make(chan abortReq, 1),
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
					addOrUpdateTaskDuration(s.taskDurations, durationID, time.Since(tRunner.startTime))
				}

				// If the node is absent, or was deleted then re-added, then we need to selectively clean up.
				// The job update is normal but we update the cluster with a fake value which denotes abnormal cleanup.
				// We need the fake value so we don't clobber any new job assignments to that nodeId.
				nodeId := nodeSt.node.Id()
				nodeStInstance, ok := s.clusterState.getNodeState(nodeId)
				nodeAbsent := !ok
				nodeReAdded := false
				if !nodeAbsent {
					nodeReAdded = (&nodeStInstance.readyCh != &nodeSt.readyCh)
				}
				nodeStChanged := nodeAbsent || nodeReAdded
				preempted := false

				if nodeStChanged {
					nodeId = nodeId + ":ERROR"
					log.WithFields(
						log.Fields{
							"node":        nodeSt.node,
							"jobID":       jobID,
							"taskID":      taskID,
							"runningJob":  nodeSt.runningJob,
							"runningTask": nodeSt.runningTask,
							"requestor":   requestor,
							"jobType":     jobType,
							"tag":         tag,
						}).Info("Task *node* lost, cleaning up.")
				}
				if nodeReAdded {
					preempted = true
				}

				flaky := false
				aborted := (err != nil && err.(*taskError).st.State == runner.ABORTED)
				if err != nil {
					// Get the type of error. Currently we only care to distinguish runner (ex: thrift) errors to mark flaky nodes.
					// TODO - we no longer set a node as flaky on failed status.
					// In practice, we've observed that this results in checkout failures causing
					// nodes to drop out of the cluster and reduce capacity to no benefit.
					// A more comprehensive solution would be to overhaul this behavior.
					taskErr := err.(*taskError)
					flaky = (taskErr.runnerErr != nil && taskErr.st.State != runner.FAILED)

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
							"jobId":     jobID,
							"taskId":    taskID,
							"err":       taskErr,
							"cmd":       strings.Join(taskDef.Argv, " "),
							"requestor": requestor,
							"jobType":   jobType,
							"tag":       tag,
						}).Info(msg)

					// If the task completed successfully but sagalog failed, start a goroutine to retry until it succeeds.
					if taskErr.sagaErr != nil && taskErr.st.RunID != "" && taskErr.runnerErr == nil && taskErr.resultErr == nil {
						log.WithFields(
							log.Fields{
								"jobId":  jobID,
								"taskId": taskID,
							}).Info(msg, " -> starting goroutine to handle failed saga.EndTask. ")
						// TODO this may result in closed channel panic due to sending endSaga to sagalog (below) before endTask
						go func() {
							for err := errors.New(""); err != nil; err = tRunner.logTaskStatus(&taskErr.st, saga.EndTask) {
								time.Sleep(time.Second)
							}
							log.WithFields(
								log.Fields{
									"jobId":     jobID,
									"taskId":    taskID,
									"requestor": requestor,
									"jobType":   jobType,
									"tag":       tag,
								}).Info(msg, " -> finished goroutine to handle failed saga.EndTask. ")
						}()
					}
				}
				if err == nil || aborted {
					log.WithFields(
						log.Fields{
							"jobId":     jobID,
							"taskId":    taskID,
							"command":   strings.Join(taskDef.Argv, " "),
							"requestor": requestor,
							"jobType":   jobType,
							"tag":       tag,
						}).Info("Ending task.")
					jobState.taskCompleted(taskID, true)
				}

				// update cluster state that this node is now free and if we consider the runner to be flaky.
				log.WithFields(
					log.Fields{
						"jobId":     jobID,
						"taskId":    taskID,
						"node":      nodeSt.node,
						"flaky":     flaky,
						"requestor": requestor,
						"jobType":   jobType,
						"tag":       tag,
					}).Info("Freeing node, removed job.")
				s.clusterState.taskCompleted(nodeId, flaky)

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
						"requestor": requestor,
						"jobType":   jobType,
						"tag":       tag,
					}).Info()
				log.WithFields(
					log.Fields{
						"running":   running,
						"completed": completed,
						"total":     total,
						"alldone":   completed == total,
						"requestor": requestor,
						"jobType":   jobType,
						"tag":       tag,
					}).Info("Jobs task summary")
			})
	}
}

// Put the kill request on channel that is processed by the main
// scheduler loop, and wait for the response
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

func (s *statefulScheduler) GetSagaCoord() saga.SagaCoordinator {
	return s.sagaCoord
}

func (s *statefulScheduler) OfflineWorker(req domain.OfflineWorkerReq) error {
	if !stringInSlice(req.Requestor, s.config.Admins) && len(s.config.Admins) != 0 {
		return fmt.Errorf("requestor %s unauthorized to offline worker", req.Requestor)
	}
	log.Infof("Offlining worker %s", req.ID)
	n := cc.NodeId(req.ID)
	if !s.clusterState.HasOnlineNode(n) {
		return fmt.Errorf("node %s was not present in nodes. It can't be offlined", req.ID)
	}

	s.clusterState.OfflineNode(n)
	return nil
}

func (s *statefulScheduler) ReinstateWorker(req domain.ReinstateWorkerReq) error {
	if !stringInSlice(req.Requestor, s.config.Admins) && len(s.config.Admins) != 0 {
		return fmt.Errorf("requestor %s unauthorized to reinstate worker", req.Requestor)
	}
	n := cc.NodeId(req.ID)
	if !s.clusterState.IsOfflined(n) {
		return fmt.Errorf("node %s was not present in offlinedNodes. It can't be reinstated", req.ID)
	} else {
		log.Infof("Reinstating worker %s", req.ID)
		s.clusterState.OnlineNode(n)
		return nil
	}
}

// process all requests verifying that the jobIds exist:  Send errors back
// immediately on the request channel for jobId that don't exist, then
// kill all the jobs with a valid ID
//
// this function is part of the main scheduler loop
func (s *statefulScheduler) killJobs() {
	defer s.stat.Latency(stats.SchedKillJobsLatency_ms).Time().Stop()
	var validKillRequests []jobKillRequest

	// validate jobids and sending invalid ids back and building a list of valid ids
	for haveKillRequest := true; haveKillRequest; {
		select {
		case req := <-s.killJobCh:
			// can we find the job?
			jobState := s.getJob(req.jobId)
			if jobState == nil {
				req.responseCh <- fmt.Errorf("cannot kill Job Id %s, not found."+
					" (This error can be ignored if kill was a fire-and-forget defensive request)."+
					" The job may be finished, "+
					" the request may still be in the queue to be scheduled, or "+
					" the id may be invalid.  "+
					" Check the job status, verify the id and/or resubmit the kill request after a few moments.",
					req.jobId)
			} else if jobState.JobKilled {
				req.responseCh <- fmt.Errorf("job Id %s was already killed, request ignored", req.jobId)
			} else {
				jobState.JobKilled = true
				validKillRequests = append(validKillRequests[:], req)
			}
		default:
			haveKillRequest = false
		}
	}

	// kill the jobs with valid ids
	s.processKillJobRequests(validKillRequests)
}

func (s *statefulScheduler) processKillJobRequests(reqs []jobKillRequest) {
	for _, req := range reqs {
		jobState := s.getJob(req.jobId)
		logFields := log.Fields{
			"jobID":     req.jobId,
			"requestor": jobState.Job.Def.Requestor,
			"jobType":   jobState.Job.Def.JobType,
			"tag":       jobState.Job.Def.Tag,
		}
		var updateMessages []saga.SagaMessage
		for _, task := range s.getJob(req.jobId).Tasks {
			msgs := s.abortTask(jobState, task, logFields, UserRequestedErrStr)
			updateMessages = append(updateMessages, msgs...)
		}

		if len(updateMessages) > 0 {
			if err := jobState.Saga.BulkMessage(updateMessages); err != nil {
				logFields["err"] = err
				log.WithFields(logFields).Error("killJobs saga.BulkMessage failure")
			}
		}

		delete(logFields, "err")
		delete(logFields, "taskID")
		log.WithFields(logFields).Info("killJobs summary")

		req.responseCh <- nil
	}
}

// abortTask abort a task - will be triggered by killing a job and when the scheduling algorithm rebalances the workers
func (s *statefulScheduler) abortTask(jobState *jobState, task *taskState, logFields log.Fields, abortMsg string) []saga.SagaMessage {
	logFields["taskID"] = task.TaskId
	log.WithFields(logFields).Info("aborting task")
	var updateMessages []saga.SagaMessage

	if task.Status == domain.InProgress {
		task.TaskRunner.Abort(true, abortMsg)
	} else if task.Status == domain.NotStarted {
		st := runner.AbortStatus("", tags.LogTags{JobID: jobState.Job.Id, TaskID: task.TaskId})
		st.Error = UserRequestedErrStr
		statusAsBytes, err := worker.SerializeProcessStatus(st)
		if err != nil {
			s.stat.Counter(stats.SchedFailedTaskSerializeCounter).Inc(1) // TODO errata metric - remove if unused
		}
		s.stat.Counter(stats.SchedCompletedTaskCounter).Inc(1)
		updateMessages = append(updateMessages, saga.MakeStartTaskMessage(jobState.Saga.ID(), task.TaskId, nil))
		updateMessages = append(updateMessages, saga.MakeEndTaskMessage(jobState.Saga.ID(), task.TaskId, statusAsBytes))
		jobState.taskCompleted(task.TaskId, false)
	}
	return updateMessages
}

func getRequestorHistory(requestorHistory *lru.Cache, requestor string) []string {
	var history []string
	iface, ok := requestorHistory.Get(requestor)
	if ok {
		history, _ = iface.([]string)
	}
	return history
}

func addOrUpdateRequestorHistory(requestorHistory *lru.Cache, requestor, newHistory string) {
	var history []string
	iface, ok := requestorHistory.Get(requestor)
	if ok {
		history, ok = iface.([]string)
		if !ok {
			return
		}
		history = append(history, newHistory)
	}
	if len(history) > DefaultMaxHistoryTags {
		history = history[1:]
	}
	requestorHistory.Add(requestor, history)
}

func addOrUpdateTaskDuration(taskDurations *lru.Cache, durationKey string, d time.Duration) {
	var ad *averageDuration
	iface, ok := taskDurations.Get(durationKey)
	if !ok {
		ad = &averageDuration{count: 1, duration: d}
		taskDurations.Add(durationKey, ad)
	} else {
		ad, ok = iface.(*averageDuration)
		if !ok {
			log.Errorf("task duration object was not *averageDuration type!  (it is %s)", reflect.TypeOf(ad))
			return
		}
		ad.update(d)
	}
}

// set the max schedulable tasks.   -1 = unlimited, 0 = don't accept any more requests, >0 = only accept job
// requests when the number of running and waiting tasks won't exceed the limit
func (s *statefulScheduler) SetSchedulerStatus(maxTasks int) error {
	err := domain.ValidateMaxTasks(maxTasks)
	if err != nil {
		return err
	}
	s.setThrottle(maxTasks)
	log.Infof("scheduler throttled to %d", maxTasks)

	if err := s.persistSettings(); err != nil {
		return fmt.Errorf("throttle setting not persisted, scheduler may use default when restarted. %s", err)
	}
	return nil
}

// return
// - true/false indicating if the scheduler is accepting job requests
// - the current number of tasks running or waiting to run
// - the max number of tasks the scheduler will handle, -1 -> there is no max number
func (s *statefulScheduler) GetSchedulerStatus() (int, int) {
	var total, completed, _ = s.getSchedulerTaskCounts()
	var task_cnt = total - completed
	return task_cnt, s.getThrottle()
}

func (s *statefulScheduler) SetSchedulingAlg(sa SchedulingAlgorithm) {
	s.config.SchedAlg = sa
}

// GetClassLoadPercents return a copy of the ClassLoadPercents
func (s *statefulScheduler) GetClassLoadPercents() (map[string]int32, error) {
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		return nil, fmt.Errorf("not using load based scheduler, no load percents")
	}
	return sa.getClassLoadPercents(), nil
}

// SetClassLoadPercents set the scheduler's class load pcts with a copy of the input class load pcts
func (s *statefulScheduler) SetClassLoadPercents(classLoadPercents map[string]int32) error {
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		return fmt.Errorf("not using load based scheduler, class load pcts ignored")
	}
	sa.setClassLoadPercents(classLoadPercents)

	if err := s.persistSettings(); err != nil {
		return fmt.Errorf("load percents not persisted, scheduler may use default when restarted. %s", err)
	}
	return nil
}

// GetRequestorToClassMap return a copy of the RequestorToClassMap
func (s *statefulScheduler) GetRequestorToClassMap() (map[string]string, error) {
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		return nil, fmt.Errorf("not using load based scheduler, no class map")
	}
	return sa.getRequestorToClassMap(), nil
}

// SetRequestorToClassMap set the scheduler's requestor to class map with a copy of the input map
func (s *statefulScheduler) SetRequestorToClassMap(requestorToClassMap map[string]string) error {
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		return fmt.Errorf("not using load based scheduler, requestor to class map ignored")
	}
	sa.setRequestorToClassMap(requestorToClassMap)

	if err := s.persistSettings(); err != nil {
		return fmt.Errorf("RequestorToClassMap not persisted, scheduler may use default when restarted. %s", err)
	}
	return nil
}

// GetRebalanceMinimumDuration
func (s *statefulScheduler) GetRebalanceMinimumDuration() (time.Duration, error) {
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		return 0, fmt.Errorf("not using load based scheduler, no rebalance min duration")
	}
	return sa.getRebalanceMinimumDuration(), nil
}

// GetRebalanceMinimumDuration
func (s *statefulScheduler) SetRebalanceMinimumDuration(durationMin time.Duration) error {
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		return fmt.Errorf("not using load based scheduler, requestor to rebalance min duration ignored")
	}
	sa.setRebalanceMinimumDuration(durationMin)

	if err := s.persistSettings(); err != nil {
		return fmt.Errorf("RebalanceMinimumDuration not persisted, scheduler may use default when restarted. %s", err)
	}
	return nil
}

// GetRebalanceThreshold
func (s *statefulScheduler) GetRebalanceThreshold() (int32, error) {
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		return 0, fmt.Errorf("not using load based scheduler, no rebalance threshold")
	}
	return int32(sa.getRebalanceThreshold()), nil
}

// SetRebalanceThreshold
func (s *statefulScheduler) SetRebalanceThreshold(threshold int32) error {
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		return fmt.Errorf("not using load based scheduler, requestor to rebalance threshold ignored")
	}
	sa.setRebalanceThreshold(int(threshold))

	if err := s.persistSettings(); err != nil {
		return fmt.Errorf("RebalanceThreshold not persisted, scheduler may use default when restarted. %s", err)
	}
	return nil
}

func (s *statefulScheduler) SetPersistor(persistor Persistor) {
	s.persistor = persistor
}

func (s *statefulScheduler) getThrottle() int {
	s.TaskThrottleMu.RLock()
	defer s.TaskThrottleMu.RUnlock()
	return s.config.TaskThrottle
}

func (s *statefulScheduler) setThrottle(throttle int) {
	s.TaskThrottleMu.Lock()
	defer s.TaskThrottleMu.Unlock()
	s.config.TaskThrottle = throttle
}
