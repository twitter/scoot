package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/golang/mock/gomock"
	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/sched/worker/workers"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

//Mocks sometimes hang without useful output, this allows early exit with err msg.
type TestTerminator struct{}

func (t *TestTerminator) Errorf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}
func (t *TestTerminator) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// objects needed to initialize a stateful scheduler
type schedulerDeps struct {
	initialCl       []cluster.Node
	clUpdates       chan []cluster.NodeUpdate
	sc              saga.SagaCoordinator
	rf              func(cluster.Node) runner.Service
	config          SchedulerConfig
	nodeToWorkerMap map[string]runner.Service
	statsRegistry   stats.StatsRegistry
}

// returns default scheduler deps populated with in memory fakes
// The default cluster has 5 nodes
func getDefaultSchedDeps() *schedulerDeps {

	tmp, _ := temp.NewTempDir("", "stateful_scheduler_test")
	cl := makeTestCluster("node1", "node2", "node3", "node4", "node5")

	return &schedulerDeps{
		initialCl: cl.nodes,
		clUpdates: cl.ch,
		sc:        sagalogs.MakeInMemorySagaCoordinatorNoGC(),
		rf: func(n cluster.Node) runner.Service {
			return workers.MakeInmemoryWorker(n, tmp)
		},
		config: SchedulerConfig{
			MaxRetriesPerTask:    0,
			DebugMode:            true,
			RecoverJobsOnStartup: false,
			DefaultTaskTimeout:   time.Second,
		},
		statsRegistry: stats.NewFinagleStatsRegistry(),
	}
}

func makeStatefulSchedulerDeps(deps *schedulerDeps) *statefulScheduler {
	statsReceiver, _ := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return deps.statsRegistry }, 0)

	s := NewStatefulScheduler(
		deps.initialCl,
		deps.clUpdates,
		deps.sc,
		deps.rf,
		deps.config,
		statsReceiver,
	)
	s.config.RunnerRetryTimeout = 0
	s.config.RunnerRetryInterval = 0
	return s
}

func makeDefaultStatefulScheduler() *statefulScheduler {
	return makeStatefulSchedulerDeps(getDefaultSchedDeps())
}

// ensure a scheduler initializes to the correct state
func Test_StatefulScheduler_Initialize(t *testing.T) {
	s := makeDefaultStatefulScheduler()

	if len(s.inProgressJobs) != 0 {
		t.Errorf("Expected Scheduler to startup with no jobs in progress")
	}

	if len(s.clusterState.nodes) != 5 {
		t.Errorf("Expected Scheduler to have a cluster with 5 nodes")
	}
}

func Test_StatefulScheduler_ScheduleJobSuccess(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, statsRegistry := initializeServices(sc, true)

	jobDef := sched.GenJobDef(1)

	go func() {
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()
	id, err := s.ScheduleJob(jobDef)
	if id == "" {
		t.Errorf("Expected successfully scheduled job to return non empty job string!")
	}

	if err != nil {
		t.Errorf("Expected job to be Scheduled Successfully %v", err)
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			stats.SchedJobsCounter:            {Checker: stats.Int64EqTest, Value: 1},
			stats.SchedJobLatency_ms + ".avg": {Checker: stats.FloatGTTest, Value: 0.0},
			stats.SchedJobRequestsCounter:     {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}
}

func Test_StatefulScheduler_ScheduleJobFailure(t *testing.T) {
	jobDef := sched.GenJobDef(1)

	// mock sagalog to trigger error
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(gomock.Any(), gomock.Any()).Return(errors.New("test error"))
	sc := saga.MakeSagaCoordinator(sagaLogMock)

	s, _, statsRegistry := initializeServices(sc, true)

	go func() {
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()
	id, err := s.ScheduleJob(jobDef)
	if id != "" {
		t.Errorf("Expected unsuccessfully scheduled job to return an empty job string!")
	}

	if err == nil {
		t.Error("Expected job return error")
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			stats.SchedJobsCounter:            {Checker: stats.DoesNotExistTest, Value: nil},
			stats.SchedJobLatency_ms + ".avg": {Checker: stats.FloatGTTest, Value: 0.0},
			stats.SchedJobRequestsCounter:     {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}
}

func Test_StatefulScheduler_AddJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, statsRegistry := initializeServices(sc, true)

	jobDef := sched.GenJobDef(2)
	go func() {
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()
	id, _ := s.ScheduleJob(jobDef)

	s.step()
	if len(s.inProgressJobs) != 1 {
		t.Errorf("Expected In Progress Jobs to be 1 not %v", len(s.inProgressJobs))
	}

	if s.getJob(id) == nil {
		t.Errorf("Expected the %v to be an inProgressJobs", id)
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			stats.SchedAcceptedJobsGauge:    {Checker: stats.Int64EqTest, Value: 1},
			stats.SchedInProgressTasksGauge: {Checker: stats.Int64EqTest, Value: 2},
			stats.SchedNumRunningTasksGauge: {Checker: stats.Int64EqTest, Value: 2},
		}) {
		t.Fatal("stats check did not pass.")
	}
}

// verifies that task gets retried maxRetryTimes and then marked as completed
func Test_StatefulScheduler_TaskGetsMarkedCompletedAfterMaxRetriesFailedStarts(t *testing.T) {
	jobDef := sched.GenJobDef(1)
	var taskIds []string
	for _, task := range jobDef.Tasks {
		taskIds = append(taskIds, task.TaskID)
	}

	taskId := taskIds[0]
	log.Info("watching", taskId)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	deps := getDefaultSchedDeps()
	deps.config.MaxRetriesPerTask = 3

	// create a runner factory that returns a runner that returns an error
	deps.rf = func(cluster.Node) runner.Service {
		chaos := runners.NewChaosRunner(nil)

		chaos.SetError(fmt.Errorf("starting error"))
		return chaos
	}

	s := makeStatefulSchedulerDeps(deps)
	go func() {
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()
	jobId, _ := s.ScheduleJob(jobDef)

	// advance scheduler until job gets scheduled & marked completed
	for len(s.inProgressJobs) == 0 || s.getJob(jobId).getJobStatus() != sched.Completed {
		s.step()
	}

	// verify task was retried enough times.
	if s.getJob(jobId).getTask(taskId).NumTimesTried != deps.config.MaxRetriesPerTask+1 {
		t.Fatalf("Expected Tries: %v times, Actual Tries: %v",
			deps.config.MaxRetriesPerTask+1, s.getJob(jobId).getTask(taskId).NumTimesTried)
	}

	// advance scheduler until job gets marked completed
	for len(s.inProgressJobs) > 0 {
		s.step()
	}
}

// verifies that task gets retried maxRetryTimes and then marked as completed
func Test_StatefulScheduler_TaskGetsMarkedCompletedAfterMaxRetriesFailedRuns(t *testing.T) {
	jobDef := sched.GenJobDef(1)
	var taskIds []string
	for _, task := range jobDef.Tasks {
		taskIds = append(taskIds, task.TaskID)
	}

	taskId := taskIds[0]
	log.Info("watching", taskId)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	deps := getDefaultSchedDeps()
	deps.config.MaxRetriesPerTask = 3

	// create a runner factory that returns a runner that always fails
	tmp, _ := temp.TempDirDefault()
	deps.rf = func(cluster.Node) runner.Service {
		ex := execers.NewDoneExecer()
		ex.ExecError = errors.New("Test - failed to exec")
		filerMap := runner.MakeRunTypeMap()
		filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: snapshots.MakeInvalidFiler(), IDC: nil}
		return runners.NewSingleRunner(ex, filerMap, runners.NewNullOutputCreator(), tmp, nil, runner.EmptyID)
	}

	s := makeStatefulSchedulerDeps(deps)
	go func() {
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()
	jobId, _ := s.ScheduleJob(jobDef)

	// advance scheduler until job gets scheduled & marked completed
	for len(s.inProgressJobs) == 0 || s.getJob(jobId).getJobStatus() != sched.Completed {
		s.step()
	}

	// verify task was retried enough times.
	if s.getJob(jobId).getTask(taskId).NumTimesTried != deps.config.MaxRetriesPerTask+1 {
		t.Fatalf("Expected Tries: %v times, Actual Tries: %v",
			deps.config.MaxRetriesPerTask+1, s.getJob(jobId).getTask(taskId).NumTimesTried)
	}

	// advance scheduler until job gets marked completed
	for len(s.inProgressJobs) > 0 {
		s.step()
	}
}

// Ensure a single job with one task runs to completion, updates
// state correctly, and makes the expected calls to the SagaLog
func Test_StatefulScheduler_JobRunsToCompletion(t *testing.T) {
	jobDef := sched.GenJobDef(1)
	var taskIds []string
	for _, task := range jobDef.Tasks {
		taskIds = append(taskIds, task.TaskID)
	}
	taskId := taskIds[0]

	deps := getDefaultSchedDeps()
	// cluster with one node
	cl := makeTestCluster("node1")
	deps.initialCl = cl.nodes
	deps.clUpdates = cl.ch

	// sagalog mock to ensure all messages are logged appropriately
	mockCtrl := gomock.NewController(&TestTerminator{})
	defer mockCtrl.Finish()
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(gomock.Any(), gomock.Any())

	deps.sc = saga.MakeSagaCoordinator(sagaLogMock)

	s := makeStatefulSchedulerDeps(deps)

	// add job and run through scheduler
	go func() {
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()
	jobId, _ := s.ScheduleJob(jobDef)

	// add additional saga data
	gomock.InOrder(
		sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage(jobId, taskId, nil)),
		sagaLogMock.EXPECT().LogMessage(
			TaskMessageMatcher{Type: &sagaStartTask, JobId: jobId, TaskId: taskId, Data: gomock.Any()}).MinTimes(0),
		sagaLogMock.EXPECT().LogMessage(
			TaskMessageMatcher{Type: &sagaEndTask, JobId: jobId, TaskId: taskId, Data: gomock.Any()}),
		sagaLogMock.EXPECT().LogMessage(saga.MakeEndSagaMessage(jobId)),
	)
	s.step()

	// advance scheduler verify task got added & scheduled
	for s.getJob(jobId).getTask(taskId).Status == sched.NotStarted {
		s.step()
	}

	// verify scheduler state updated appropriately
	if s.clusterState.nodes["node1"].runningTask != taskId {
		t.Errorf("Expected %v to be scheduled on node1.  nodestate: %+v", taskId, s.clusterState.nodes["node1"])
	}

	// advance scheduler until the task completes
	for s.getJob(jobId).getTask(taskId).Status == sched.InProgress {
		s.step()
	}

	// verify state changed appropriately
	if s.clusterState.nodes["node1"].runningTask != noTask {
		t.Errorf("Expected node1 to not have any running tasks")
	}

	// advance scheduler until job gets marked completed
	for s.getJob(jobId).getJobStatus() != sched.Completed {
		s.step()
	}

	// verify that EndSaga Message gets logged
	if !s.getJob(jobId).EndingSaga {
		t.Errorf("Expected Completed job to be EndingSaga")
	}

	for len(s.inProgressJobs) > 0 {
		s.step()
	}

}

func Test_StatefulScheduler_KillStartedJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, _ := initializeServices(sc, false)

	jobId, taskIds, _ := putJobInScheduler(1, s, "pause", "", sched.P0)
	s.step() // get the first job in the queue
	for s.getJob(jobId).getTask(taskIds[0]).Status == sched.NotStarted {
		s.step()
	}

	respCh := sendKillRequest(jobId, s)

	for s.getJob(jobId).getTask(taskIds[0]).Status == sched.InProgress ||
		s.getJob(jobId).getTask(taskIds[0]).Status == sched.NotStarted {
		s.step()
	}
	errResp := <-respCh

	if errResp != nil {
		t.Fatalf("Expected no error from killJob request, instead got:%s", errResp.Error())
	}
	verifyJobStatus("verify kill", jobId, sched.Completed, []sched.Status{sched.Completed}, s, t)
}

func Test_StatefulScheduler_KillNotFoundJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, _ := initializeServices(sc, false)
	putJobInScheduler(1, s, "pause", "", sched.P0)

	respCh := sendKillRequest("badJobId", s)

	err := waitForResponse(respCh, s)

	if err == nil {
		t.Errorf("Expected to get job not found error, instead got nil")
	}

	log.Infof("Got job not found error: \n%s", err.Error())

}

func Test_StatefulScheduler_KillFinishedJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, _ := initializeServices(sc, true)
	jobId, taskIds, _ := putJobInScheduler(1, s, "", "", sched.P0)
	s.step() // get the job in the queue

	//advance scheduler until the task completes
	for s.getJob(jobId).getTask(taskIds[0]).Status == sched.InProgress {
		s.step()
	}

	// verify state changed appropriately
	for i := 0; i < 5; i++ {
		if s.clusterState.nodes[cluster.NodeId(fmt.Sprintf("node%d", i+1))].runningTask != noTask {
			t.Errorf("Expected nodes to not have any running tasks")
		}
	}

	// advance scheduler until job gets marked completed
	for s.getJob(jobId).getJobStatus() != sched.Completed {
		s.step()
	}

	respCh := sendKillRequest(jobId, s)

	err := waitForResponse(respCh, s)

	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("Expected err to be nil, instead is %v", err.Error())
		}
	} else {
		j := s.getJob(jobId)
		for j != nil {
			s.step()
			j = s.getJob(jobId)
		}
	}
}

func Test_StatefulScheduler_KillNotStartedJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, statsRegistry := initializeServices(sc, false)

	// create a job with 5 pausing tasks and get them all to InProgress state
	jobId1, _, _ := putJobInScheduler(5, s, "pause", "", sched.P0)
	s.step()
	for !allTasksInState("job1", jobId1, s, sched.InProgress) {
		s.step()
	}

	verifyJobStatus("verify started job1", jobId1, sched.InProgress,
		[]sched.Status{sched.InProgress, sched.InProgress, sched.InProgress, sched.InProgress, sched.InProgress}, s, t)

	if !stats.StatsOk("first stage", statsRegistry, t,
		map[string]stats.Rule{
			stats.SchedAcceptedJobsGauge: {Checker: stats.Int64EqTest, Value: 1},
			stats.SchedWaitingJobsGauge:  {Checker: stats.Int64EqTest, Value: 0},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// put a job with 3 tasks in the queue - all tasks should be in NotStarted state
	jobId2, _, _ := putJobInScheduler(3, s, "pause", "", sched.P0)
	s.step()
	verifyJobStatus("verify put job2 in scheduler", jobId2, sched.InProgress,
		[]sched.Status{sched.NotStarted, sched.NotStarted, sched.NotStarted}, s, t)

	if !stats.StatsOk("second stage", statsRegistry, t,
		map[string]stats.Rule{
			stats.SchedAcceptedJobsGauge: {Checker: stats.Int64EqTest, Value: 2},
			stats.SchedWaitingJobsGauge:  {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// kill the second job twice to verify the killed 2x error message
	respCh := sendKillRequest(jobId2, s)
	respCh2 := sendKillRequest(jobId2, s)

	// pause to let the scheduler pick up the kill requests, then step once to process them.
	time.Sleep(500 * time.Millisecond)
	s.step()

	// wait for a response from the first kill and check for one err == nil and one err != nil
	err := waitForResponse(respCh, s)
	err2 := waitForResponse(respCh2, s)

	if (err == nil) == (err2 == nil) {
		t.Errorf("Expected one nil and one non-nil err when killing job twice, got: %v, %v", err, err2)
	} else if err == nil && !strings.Contains(err2.Error(), "was already killed,") {
		t.Errorf("Killed a job twice, expected the error to contain 'was already killed', got %s", err2.Error())
	} else if err != nil && !strings.Contains(err.Error(), "was already killed,") {
		t.Errorf("Killed a job twice, expected the error to contain 'was already killed', got %s", err.Error())
	}

	// verify that the first job is still running
	verifyJobStatus("verify job1 still running", jobId1, sched.InProgress,
		[]sched.Status{sched.InProgress, sched.InProgress, sched.InProgress, sched.InProgress, sched.InProgress}, s, t)

	// cleanup
	sendKillRequest(jobId1, s)
}

func Test_StatefulScheduler_NodeScaleFactor(t *testing.T) {
	NodeScaleAdjustment = []float32{.05, .2, .75} // Setting this global value explicitly for test consistency.
	s := &SchedulerConfig{SoftMaxSchedulableTasks: 200}
	numNodes := 21
	numTasks := float32(1)
	if n := ceil(numTasks * s.GetNodeScaleFactor(numNodes, 0)); n != 1 {
		t.Errorf("Expected 1, got %d", n)
	}

	numTasks = float32(100)
	if n := ceil(numTasks * s.GetNodeScaleFactor(numNodes, 0)); n != 1 {
		t.Errorf("Expected 1, got %d", n)
	}
	if n := ceil(numTasks * s.GetNodeScaleFactor(numNodes, 1)); n != 3 {
		t.Errorf("Expected 3, got %d", n)
	}
	if n := ceil(numTasks * s.GetNodeScaleFactor(numNodes, 2)); n != 8 {
		t.Errorf("Expected 8, got %d", n)
	}
}

func Test_StatefulScheduler_Throttle_Error(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, _ := initializeServices(sc, false)

	err := s.SetSchedulerStatus(-10)
	expected := "invalid tasks limit:-10. Must be >= -1."
	if strings.Compare(expected, fmt.Sprintf("%s", err)) != 0 {
		t.Fatalf("expected: %s, got: %s", expected, err)
	}
}
func Test_StatefulScheduler_GetThrottledStatus(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, _ := initializeServices(sc, false)

	s.SetSchedulerStatus(0)

	var num_tasks, max_tasks int
	num_tasks, max_tasks = s.GetSchedulerStatus()
	if num_tasks != 0 || max_tasks != 0 {
		t.Fatalf("GetSchedulerStatus: expected: 0, 0, got: %d, %d",
			num_tasks, max_tasks)
	}
}

func Test_StatefulScheduler_GetNotThrottledStatus(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, _ := initializeServices(sc, false)

	s.SetSchedulerStatus(-1)

	var num_tasks, max_tasks int
	num_tasks, max_tasks = s.GetSchedulerStatus()
	if num_tasks != 0 || max_tasks != -1 {
		t.Fatalf("GetSchedulerStatus: expected: 0, -1, got: %d, %d",
			num_tasks, max_tasks)
	}
}

func Test_StatefulScheduler_GetSomeThrottledStatus(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, _ := initializeServices(sc, false)

	s.SetSchedulerStatus(10)

	var num_tasks, max_tasks int
	num_tasks, max_tasks = s.GetSchedulerStatus()
	if num_tasks != 0 || max_tasks != 10 {
		t.Fatalf("GetSchedulerStatus: expected: 0, 10, got: %d, %d",
			num_tasks, max_tasks)
	}
}

func checkGauges(requestor string, expectedCounts map[string]int, s *statefulScheduler,
	t *testing.T, statsRegistry stats.StatsRegistry) bool {
	// check the gauges
	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("%s_%s", stats.SchedNumRunningJobsGauge, requestor):  {Checker: stats.Int64EqTest, Value: expectedCounts["jobRunning"]},
			fmt.Sprintf("%s_%s", stats.SchedWaitingJobsGauge, requestor):     {Checker: stats.Int64EqTest, Value: expectedCounts["jobsWaitingToStart"]},
			fmt.Sprintf("%s_%s", stats.SchedNumRunningTasksGauge, requestor): {Checker: stats.Int64EqTest, Value: expectedCounts["numRunningTasks"]},
			fmt.Sprintf("%s_%s", stats.SchedNumWaitingTasksGauge, requestor): {Checker: stats.Int64EqTest, Value: expectedCounts["numWaitingTasks"]},
		}) {
		return false
	}

	return true
}

func allTasksInState(jobName string, jobId string, s *statefulScheduler, status sched.Status) bool {
	for _, task := range s.getJob(jobId).Tasks {
		if task.Status != status {
			return false
		}
	}

	return true
}

func waitForResponse(respCh chan error, s *statefulScheduler) error {
	var lookingForResp bool = true
	for lookingForResp {
		select {
		case err := <-respCh:
			return err
		default:
			s.step()
		}
	}

	return nil
}

func sendKillRequest(jobId string, s *statefulScheduler) chan error {

	respCh := make(chan error)
	go func(respCh chan error) {
		respCh <- s.KillJob(jobId)
	}(respCh)

	return respCh
}

func initializeServices(sc saga.SagaCoordinator, useDefaultDeps bool) (*statefulScheduler, []*execers.SimExecer, stats.StatsRegistry) {
	var deps *schedulerDeps
	var exs []*execers.SimExecer
	if useDefaultDeps {
		deps = getDefaultSchedDeps()
	} else {
		deps, exs = getDepsWithSimWorker()
	}

	deps.sc = sc
	return makeStatefulSchedulerDeps(deps), exs, deps.statsRegistry
}

// create a job definition containing numTasks tasks and put it in the scheduler.
// usingPausingExecer is true, each task will contain the command "pause"
func putJobInScheduler(numTasks int, s *statefulScheduler, command string,
	requestor string, priority sched.Priority) (string, []string, error) {
	// create the job and run it to completion
	// create the job and run it to completion
	jobDef := sched.GenJobDef(numTasks)
	jobDef.Requestor = requestor
	jobDef.Priority = priority

	var taskIds []string

	if command != "" {
		//change command to pause.
		for i := range jobDef.Tasks {
			// set the command to pause
			jobDef.Tasks[i].Argv = []string{command}
		}
	}

	for _, task := range jobDef.Tasks {
		taskIds = append(taskIds, task.TaskID)
	}

	// put the job on the jobs channel
	go func() {
		// simulate checking the job and returning no error, so ScheduleJob() will put the job definition
		// immediately on the addJobCh
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()
	jobId, err := s.ScheduleJob(jobDef)

	return jobId, taskIds, err
}

func verifyJobStatus(tag string, jobId string, expectedJobStatus sched.Status, expectedTaskStatus []sched.Status,
	s *statefulScheduler, t *testing.T) bool {

	jobStatus := s.getJob(jobId)

	if jobStatus.getJobStatus() != expectedJobStatus {
		t.Errorf("%s: Expected job status to be %s, got %s", tag, expectedJobStatus.String(), jobStatus.getJobStatus().String())
		return false
	}

	i := 0
	for _, task := range jobStatus.Tasks {
		if task.Status != expectedTaskStatus[i] {
			t.Errorf("%s: Expected task %d status to be %s, got %s", tag, i, expectedTaskStatus[i].String(), task.Status.String())
			return false
		}
		i++
	}

	return true

}

func getDepsWithSimWorker() (*schedulerDeps, []*execers.SimExecer) {

	tmp, _ := temp.NewTempDir("", "stateful_scheduler_test")
	cl := makeTestCluster("node1", "node2", "node3", "node4", "node5")

	return &schedulerDeps{
		initialCl: cl.nodes,
		clUpdates: cl.ch,
		sc:        sagalogs.MakeInMemorySagaCoordinatorNoGC(),
		rf: func(n cluster.Node) runner.Service {
			ex := execers.NewSimExecer()
			filerMap := runner.MakeRunTypeMap()
			filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: snapshots.MakeInvalidFiler(), IDC: nil}
			runner := runners.NewSingleRunner(ex, filerMap, runners.NewNullOutputCreator(), tmp, nil, runner.EmptyID)
			return runner
		},
		config: SchedulerConfig{
			MaxRetriesPerTask:    0,
			DebugMode:            true,
			RecoverJobsOnStartup: false,
			DefaultTaskTimeout:   time.Second,
		},
		statsRegistry: stats.NewFinagleStatsRegistry(),
	}, nil

}
