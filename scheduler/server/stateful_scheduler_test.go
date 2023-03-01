package server

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	lru "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	cc "github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/scheduler/domain"
	"github.com/twitter/scoot/scheduler/setup/worker"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
	"github.com/twitter/scoot/tests/testhelpers"
)

// Mocks sometimes hang without useful output, this allows early exit with err msg.
type TestTerminator struct{}

func (t *TestTerminator) Errorf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}
func (t *TestTerminator) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// objects needed to initialize a stateful scheduler
type schedulerDeps struct {
	nodesUpdatesCh chan []cc.NodeUpdate
	sc             saga.SagaCoordinator
	rf             func(cc.Node) runner.Service
	config         SchedulerConfiguration
	statsRegistry  stats.StatsRegistry
}

// returns default scheduler deps populated with in memory fakes
// The default cluster has 5 nodes
func getDefaultSchedDeps() *schedulerDeps {
	nodeUpdateCh := make(chan []cc.NodeUpdate, common.DefaultClusterChanSize)
	initTestCluster(nodeUpdateCh, "node1", "node2", "node3", "node4", "node5")

	return &schedulerDeps{
		nodesUpdatesCh: nodeUpdateCh,
		sc:             sagalogs.MakeInMemorySagaCoordinatorNoGC(nil),
		rf: func(n cc.Node) runner.Service {
			return worker.MakeInmemoryWorker(n)
		},
		config: SchedulerConfiguration{
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
		deps.nodesUpdatesCh,
		deps.sc,
		deps.rf,
		deps.config,
		statsReceiver,
		nil,
		nil,
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

	validateCompletionCounts(s, t)
}

func Test_StatefulScheduler_ScheduleJobSuccess(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, statsRegistry := initializeServices(sc, true)

	jobDef := domain.GenJobDef(1)

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

	validateCompletionCounts(s, t)
}

func Test_StatefulScheduler_ScheduleJobFailure(t *testing.T) {
	jobDef := domain.GenJobDef(1)

	// mock sagalog to trigger error
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(gomock.Any(), gomock.Any()).Return(errors.New("test error"))
	sc := saga.MakeSagaCoordinator(sagaLogMock, nil)

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

	validateCompletionCounts(s, t)
}

func Test_StatefulScheduler_AddJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, statsRegistry := initializeServices(sc, true)

	jobDef := domain.GenJobDef(2)
	jobDef.Requestor = "fakeRequestor"
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
			stats.SchedAcceptedJobsGauge:                                     {Checker: stats.Int64EqTest, Value: 1},
			stats.SchedInProgressTasksGauge:                                  {Checker: stats.Int64EqTest, Value: 2},
			stats.SchedNumRunningTasksGauge:                                  {Checker: stats.Int64EqTest, Value: 2},
			fmt.Sprintf("%s_fakeRequestor", stats.SchedInProgressTasksGauge): {Checker: stats.Int64EqTest, Value: 1},
			fmt.Sprintf("%s_fakeRequestor", stats.SchedInProgressTasksGauge): {Checker: stats.Int64EqTest, Value: 2},
			fmt.Sprintf("%s_fakeRequestor", stats.SchedNumRunningTasksGauge): {Checker: stats.Int64EqTest, Value: 2},
		}) {
		t.Fatal("stats check did not pass.")
	}

	validateCompletionCounts(s, t)
}

// verifies that task gets retried maxRetryTimes and then marked as completed
func Test_StatefulScheduler_TaskGetsMarkedCompletedAfterMaxRetriesFailedStarts(t *testing.T) {
	jobDef := domain.GenJobDef(1)
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
	deps.rf = func(cc.Node) runner.Service {
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
	for len(s.inProgressJobs) == 0 || s.getJob(jobId).getJobStatus() != domain.Completed {
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

	validateCompletionCounts(s, t)
}

// verifies that task gets retried maxRetryTimes and then marked as completed
func Test_StatefulScheduler_TaskGetsMarkedCompletedAfterMaxRetriesFailedRuns(t *testing.T) {
	jobDef := domain.GenJobDef(1)
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
	deps.rf = func(cc.Node) runner.Service {
		ex := execers.NewDoneExecer()
		ex.ExecError = errors.New("Test - failed to exec")
		filerMap := runner.MakeRunTypeMap()
		filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: snapshots.MakeInvalidFiler(), IDC: nil}
		return runners.NewSingleRunner(ex, filerMap, runners.NewNullOutputCreator(), nil, stats.NopDirsMonitor, runner.EmptyID, []func() error{}, []func() error{}, nil)
	}

	s := makeStatefulSchedulerDeps(deps)
	go func() {
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()
	jobId, _ := s.ScheduleJob(jobDef)

	// advance scheduler until job gets scheduled & marked completed
	for len(s.inProgressJobs) == 0 || s.getJob(jobId).getJobStatus() != domain.Completed {
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

	validateCompletionCounts(s, t)
}

// Ensure a single job with one task runs to completion, updates
// state correctly, and makes the expected calls to the SagaLog
func Test_StatefulScheduler_JobRunsToCompletion(t *testing.T) {
	jobDef := domain.GenJobDef(1)
	var taskIds []string
	for _, task := range jobDef.Tasks {
		taskIds = append(taskIds, task.TaskID)
	}
	taskId := taskIds[0]

	deps := getDefaultSchedDeps()
	// cluster with one node
	deps.nodesUpdatesCh <- []cc.NodeUpdate{cc.NewAdd(cc.NewIdNode("node1"))}

	// sagalog mock to ensure all messages are logged appropriately
	mockCtrl := gomock.NewController(&TestTerminator{})
	defer mockCtrl.Finish()
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(gomock.Any(), gomock.Any())
	sagaLogMock.EXPECT().GetActiveSagas().AnyTimes()

	deps.sc = saga.MakeSagaCoordinator(sagaLogMock, nil)

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
	for s.getJob(jobId).getTask(taskId).Status == domain.NotStarted {
		s.step()
	}

	var assignedNode *nodeState
	for _, node := range s.clusterState.nodes {
		if node.runningTask == taskId {
			assignedNode = node
		}
	}

	// advance scheduler until the task completes
	for s.getJob(jobId).getTask(taskId).Status == domain.InProgress {
		s.step()
	}

	// verify state changed appropriately
	if assignedNode.runningTask != noTask {
		t.Errorf("Expected node1 to not have any running tasks")
	}

	// advance scheduler until job gets marked completed
	for s.getJob(jobId).getJobStatus() != domain.Completed {
		s.step()
	}

	// verify that EndSaga Message gets logged
	if !s.getJob(jobId).EndingSaga {
		t.Errorf("Expected Completed job to be EndingSaga")
	}

	for len(s.inProgressJobs) > 0 {
		s.step()
	}

	validateCompletionCounts(s, t)
}

func Test_StatefulScheduler_KillStartedJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, _ := initializeServices(sc, false)

	jobId, taskIds, _ := putJobInScheduler(1, s, "pause", "", domain.P0)
	s.step() // get the first job in the queue
	for s.getJob(jobId).getTask(taskIds[0]).Status == domain.NotStarted {
		s.step()
	}

	respCh := sendKillRequest(jobId, s)

	for s.getJob(jobId).getTask(taskIds[0]).Status == domain.InProgress ||
		s.getJob(jobId).getTask(taskIds[0]).Status == domain.NotStarted {
		s.step()
	}
	errResp := <-respCh

	if errResp != nil {
		t.Fatalf("Expected no error from killJob request, instead got:%s", errResp.Error())
	}
	verifyJobStatus("verify kill", jobId, domain.Completed, []domain.Status{domain.Completed}, s, t)

	validateCompletionCounts(s, t)
}

func Test_StatefulScheduler_KillNotFoundJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, _ := initializeServices(sc, false)
	putJobInScheduler(1, s, "pause", "", domain.P0)

	respCh := sendKillRequest("badJobId", s)

	err := waitForResponse(respCh, s)

	if err == nil {
		t.Errorf("Expected to get job not found error, instead got nil")
	}

	log.Infof("Got job not found error: \n%s", err.Error())

	validateCompletionCounts(s, t)
}

func Test_StatefulScheduler_KillFinishedJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, _ := initializeServices(sc, true)
	jobId, taskIds, _ := putJobInScheduler(1, s, "", "", domain.P0)
	s.step() // get the job in the queue

	//advance scheduler until the task completes
	for s.getJob(jobId).getTask(taskIds[0]).Status == domain.InProgress {
		s.step()
	}

	// verify state changed appropriately
	for i := 0; i < 5; i++ {
		if s.clusterState.nodes[cc.NodeId(fmt.Sprintf("node%d", i+1))].runningTask != noTask {
			t.Errorf("Expected nodes to not have any running tasks")
		}
	}

	// advance scheduler until job gets marked completed
	for s.getJob(jobId).getJobStatus() != domain.Completed {
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

	validateCompletionCounts(s, t)
}

func Test_StatefulScheduler_KillNotStartedJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, statsRegistry := initializeServices(sc, false)

	// create a job with 5 pausing tasks and get them all to InProgress state
	jobId1, _, _ := putJobInScheduler(5, s, "pause", "", domain.P0)
	s.step()
	for !allTasksInState("job1", jobId1, s, domain.InProgress) {
		s.step()
	}

	verifyJobStatus("verify started job1", jobId1, domain.InProgress,
		[]domain.Status{domain.InProgress, domain.InProgress, domain.InProgress, domain.InProgress, domain.InProgress}, s, t)

	if !stats.StatsOk("first stage", statsRegistry, t,
		map[string]stats.Rule{
			stats.SchedAcceptedJobsGauge: {Checker: stats.Int64EqTest, Value: 1},
			stats.SchedWaitingJobsGauge:  {Checker: stats.Int64EqTest, Value: 0},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// put a job with 3 tasks in the queue - all tasks should be in NotStarted state
	jobId2, _, _ := putJobInScheduler(3, s, "pause", "", domain.P0)
	s.step()
	verifyJobStatus("verify put job2 in scheduler", jobId2, domain.InProgress,
		[]domain.Status{domain.NotStarted, domain.NotStarted, domain.NotStarted}, s, t)

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
	verifyJobStatus("verify job1 still running", jobId1, domain.InProgress,
		[]domain.Status{domain.InProgress, domain.InProgress, domain.InProgress, domain.InProgress, domain.InProgress}, s, t)

	// cleanup
	sendKillRequest(jobId1, s)

	validateCompletionCounts(s, t)
}

func Test_StatefulScheduler_Throttle_Error(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, _ := initializeServices(sc, false)

	err := s.SetSchedulerStatus(-10)
	expected := "invalid tasks limit:-10. Must be >= -1."
	if strings.Compare(expected, fmt.Sprintf("%s", err)) != 0 {
		t.Fatalf("expected: %s, got: %s", expected, err)
	}
}
func Test_StatefulScheduler_GetThrottledStatus(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
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
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
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
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, _ := initializeServices(sc, false)

	s.SetSchedulerStatus(10)

	var num_tasks, max_tasks int
	num_tasks, max_tasks = s.GetSchedulerStatus()
	if num_tasks != 0 || max_tasks != 10 {
		t.Fatalf("GetSchedulerStatus: expected: 0, 10, got: %d, %d",
			num_tasks, max_tasks)
	}
}

func TestUpdateAvgDuration(t *testing.T) {
	taskDurations, err := lru.New(3)
	if err != nil {
		t.Fatalf("Failed to create LRU: %v", err)
	}

	// verify durations are tracked correctly

	addOrUpdateTaskDuration(taskDurations, "foo", 5*time.Second)
	addOrUpdateTaskDuration(taskDurations, "foo", 21*time.Second)
	addOrUpdateTaskDuration(taskDurations, "foo", 25*time.Second)

	iface, ok := taskDurations.Get("foo")
	if !ok {
		t.Fatal("foo wasn't in taskDurations")
	}
	ad, ok := iface.(*averageDuration)
	if !ok {
		t.Fatal("Failed iface assertion to *averageDuration")
	}
	if ad.duration != 17*time.Second {
		t.Fatalf("Expected 17 seconds, got %v", ad.duration)
	}

	// verify lru size limit

	if taskDurations.Len() != 1 {
		t.Fatalf("Expected taskDurations len: 1, got: %d", taskDurations.Len())
	}

	addOrUpdateTaskDuration(taskDurations, "bar", 5*time.Second)
	addOrUpdateTaskDuration(taskDurations, "baz", 5*time.Second)
	addOrUpdateTaskDuration(taskDurations, "oof", 5*time.Second)

	if taskDurations.Len() != 3 {
		t.Fatalf("Expected taskDurations len: 3, got: %d", taskDurations.Len())
	}
}

// Respectable figures from a 2018 13" Macbook Pro 2.7Ghz Quad core i7:
// tasksInJob = 100:   181 iterations, ~6,000,000 ns/op
// tasksInJob = 1000:  50  iterations, ~22,000,000 ns/op
// tasksInJob = 10000: 4   iterations, ~300,000,000 ns/op
func BenchmarkProcessKillJobsRequests(b *testing.B) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, _ := initializeServices(sc, false)
	tasksInJob := 10000

	for i := 0; i < b.N; i++ {
		jobId, _, _ := putJobInScheduler(tasksInJob, s, "pause", "", domain.P0)
		s.step()

		validKillRequests := []jobKillRequest{{jobId: jobId, responseCh: make(chan error, 1)}}
		s.processKillJobRequests(validKillRequests)
	}
}

func allTasksInState(jobName string, jobId string, s *statefulScheduler, status domain.Status) bool {
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
	requestor string, priority domain.Priority) (string, []string, error) {
	// create the job and run it to completion
	jobDef := domain.GenJobDef(numTasks)
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

func verifyJobStatus(tag string, jobId string, expectedJobStatus domain.Status, expectedTaskStatus []domain.Status,
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

	uc := initNodeUpdateChan("node1", "node2", "node3", "node4", "node5")

	return &schedulerDeps{
		nodesUpdatesCh: uc,
		sc:             sagalogs.MakeInMemorySagaCoordinatorNoGC(nil),
		rf: func(n cc.Node) runner.Service {
			ex := execers.NewSimExecer()
			filerMap := runner.MakeRunTypeMap()
			filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: snapshots.MakeInvalidFiler(), IDC: nil}
			runner := runners.NewSingleRunner(ex, filerMap, runners.NewNullOutputCreator(), nil, stats.NopDirsMonitor, runner.EmptyID, []func() error{}, []func() error{}, nil)
			return runner
		},
		config: SchedulerConfiguration{
			MaxRetriesPerTask:    0,
			DebugMode:            true,
			RecoverJobsOnStartup: false,
			DefaultTaskTimeout:   time.Second,
		},
		statsRegistry: stats.NewFinagleStatsRegistry(),
	}, nil
}

func validateCompletionCounts(s *statefulScheduler, t *testing.T) {

	// check the counts of entries in tasksByJobClassAndStartTimeSec map and counts within each jobState
	for _, js := range s.inProgressJobs {
		mapRunning := 0
		for classNStartKey, v := range js.tasksByJobClassAndStartTimeSec {
			if classNStartKey.class != js.jobClass {
				continue
			}
			for jobIDnTaskID := range v {
				if jobIDnTaskID.jobID != js.Job.Id {
					continue
				}
				mapRunning++
			}
		}

		jRunning := 0
		jCompleted := 0
		jNotStarted := 0
		for _, taskState := range js.Tasks {
			if taskState.Status == domain.InProgress {
				jRunning++
			} else if taskState.Status == domain.NotStarted {
				jNotStarted++
			} else if taskState.Status == domain.Completed {
				jCompleted++
			}
		}
		// job's running task counts:
		// taskStates in running state vs js count of tasks in running state
		assert.Equal(t, jRunning, js.TasksRunning,
			"job  %s,%s,%s, has %d tasks in running state but %s running task count",
			jRunning, js.jobClass, js.Job.Def.Requestor, js.Job.Id, jRunning, js.TasksRunning)
		// the map's (running) task count vs job state's running task count
		assert.Equal(t, mapRunning, js.TasksRunning, "job:%s,%s,%s has %d tasks running in the map, but %d in its TasksRunning count",
			js.jobClass, js.Job.Def.Requestor, js.Job.Id, mapRunning, js.TasksRunning)

		// jobState's completed task counts:
		// taskStates in completed state vs js count of tasks in completed state
		assert.Equal(t, jCompleted, js.TasksCompleted,
			"job %s,%s,%s has %d tasks in completed state but a count of %d completed tasks",
			jRunning, js.jobClass, js.Job.Def.Requestor, js.Job.Id, jCompleted, js.TasksCompleted)
	}

	// total running in map vs total running for scheduler
	totalMapRunning := 0
	for _, v := range s.tasksByJobClassAndStartTimeSec {
		totalMapRunning += len(v)
	}
	_, _, running := s.getSchedulerTaskCounts()
	assert.Equal(t, totalMapRunning, running, "running count from tasksByJobClassAndStartTimeSec map (%d) is not equal to running from scheduler (%d)",
		totalMapRunning, running)
}

func Test_StatefulScheduler_RequestorCountsStats(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, _ := initializeServices(sc, false)
	s.SetClassLoadPercents(map[string]int32{"fake R1": 60, "fake R2": 40})
	s.SetRequestorToClassMap(map[string]string{"fake R1": "fake R1", "fake R2": "fake R2"})

	requestors := []string{"fake R1", "fake R1", "fake R1", "fake R2", "fake R2"}
	// put 5 jobs in the queue
	for i := 0; i < 5; i++ {
		jobDef := domain.GenJobDef((i + 1))
		for j := 0; j < len(jobDef.Tasks); j++ {
			jobDef.Tasks[j].TaskID = fmt.Sprintf("task: %d", j)
		}
		jobDef.Requestor = requestors[i]
		jobDef.Priority = domain.P0

		go func() {
			// simulate checking the job and returning no error, so ScheduleJob() will put the job definition
			// immediately on the addJobCh
			checkJobMsg := <-s.checkJobCh
			checkJobMsg.resultCh <- nil
		}()

		s.ScheduleJob(jobDef)
		s.addJobs()
	}

	s.step()

	tmp := string(s.stat.Render(true))
	fmt.Println(tmp)
	assert.True(t, strings.Contains(tmp, "\"schedInProgressTasksGauge\": 15"))
	assert.True(t, strings.Contains(tmp, "\"schedInProgressTasksGauge_fake R1\": 6"))
	assert.True(t, strings.Contains(tmp, "\"schedInProgressTasksGauge_fake R2\": 9"))
	assert.True(t, strings.Contains(tmp, "\"schedNumRunningTasksGauge\": 5"))
	assert.True(t, strings.Contains(tmp, "\"schedNumRunningTasksGauge_fake R1\": 3"))
	assert.True(t, strings.Contains(tmp, "\"schedNumRunningTasksGauge_fake R2\": 2"))
	assert.True(t, strings.Contains(tmp, "\"schedNumWaitingTasksGauge\": 10"))
	assert.True(t, strings.Contains(tmp, "\"schedNumWaitingTasksGauge_fake R1\": 3"))
	assert.True(t, strings.Contains(tmp, "\"schedNumWaitingTasksGauge_fake R2\": 7"))
}

func Test_StatefulScheduler_TaskDurationOrdering_Durations(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, _ := initializeServices(sc, true)
	s.SetClassLoadPercents(map[string]int32{"fake R1": 100})
	s.SetRequestorToClassMap(map[string]string{"fake R1": "fake R1"})

	// validate when already have duration
	for i := 1; i < 5; i++ {
		addOrUpdateTaskDuration(s.taskDurations, fmt.Sprintf("task: %d", i), time.Duration(i*10)*time.Second)
	}

	// put 5 jobs in the queue
	for i := 0; i < 5; i++ {
		jobDef := domain.GenJobDef((i + 1))
		for j := 0; j < len(jobDef.Tasks); j++ {
			jobDef.Tasks[j].TaskID = fmt.Sprintf("task: %d", j)
		}
		jobDef.Requestor = "fake R1"
		jobDef.Priority = domain.P0

		go func() {
			// simulate checking the job and returning no error, so ScheduleJob() will put the job definition
			// immediately on the addJobCh
			checkJobMsg := <-s.checkJobCh
			checkJobMsg.resultCh <- nil
		}()

		s.ScheduleJob(jobDef)
		s.addJobs()
	}

	s.step()

	for i := 0; i < 5; i++ {
		js := s.inProgressJobs[i]
		for j := 0; j < len(js.Tasks); j++ {
			task := js.Tasks[j]
			expectedTaskId := fmt.Sprintf("task: %d", len(js.Tasks)-j-1)
			assert.Equal(t, expectedTaskId, task.TaskId, fmt.Sprintf("expected job %d, task[%d] to be %s, got %s",
				i, j, expectedTaskId, task.TaskId))
		}
		priorDuration := js.Tasks[0].AvgDuration
		for j := 1; j < len(js.Tasks); j++ {
			task := js.Tasks[j]
			assert.True(t, priorDuration >= task.AvgDuration, fmt.Sprintf("expected job %d, %s, duration %d to be <= than %d",
				i, task.TaskId, task.AvgDuration, priorDuration))
			priorDuration = js.Tasks[j].AvgDuration
		}
	}
}

// Test creating job definitions with tasks in descending duration order
func Test_TaskAssignments_TasksScheduledByDuration(t *testing.T) {
	// create a test cluster with 3 nodes
	uc := initNodeUpdateChan("node1", "node2", "node3")
	s := getDebugStatefulScheduler(uc)
	taskKeyFn := func(key string) string {
		keyParts := strings.Split(key, " ")
		return keyParts[len(keyParts)-1]
	}
	s.durationKeyExtractorFn = taskKeyFn

	// create a jobdef with 10 tasks
	job := domain.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 10)

	// set the scheduler's current (fake) duration data
	for i := range job.Def.Tasks {
		// update TaskID to match what we are really seeing from our clients (GenJob() generates different values needed by other tests)
		job.Def.Tasks[i].TaskID = strings.Join(job.Def.Tasks[i].Argv, " ")
		addOrUpdateTaskDuration(s.taskDurations, s.durationKeyExtractorFn(job.Def.Tasks[i].TaskID), time.Duration(i)*time.Second)
	}
	go func() {
		// simulate checking the job and returning no error, so ScheduleJob() will put the job definition
		// immediately on the addJobCh
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()

	s.ScheduleJob(job.Def)
	s.addJobs()

	js1 := s.inProgressJobs[0]
	// verify tasks are in descending duration order
	for i, task := range js1.Tasks {
		assert.True(t, task.AvgDuration != time.Duration(math.MaxInt64), "average duration not found for task %d, %v", i, task)
		if i == 0 {
			continue
		}
		assert.True(t, js1.Tasks[i-1].AvgDuration >= task.AvgDuration, fmt.Sprintf("tasks not in descending duration order at task %d, %v", i, task))
	}
}

func initNodeUpdateChan(nodes ...string) chan []cc.NodeUpdate {
	uc := make(chan []cc.NodeUpdate, common.DefaultClusterChanSize)
	updates := []cc.NodeUpdate{}
	for _, node := range nodes {
		updates = append(updates, cc.NewAdd(cc.NewIdNode(node)))
	}
	uc <- updates
	return uc
}
