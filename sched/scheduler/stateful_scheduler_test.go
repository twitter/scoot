package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/golang/mock/gomock"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/saga/sagalogs"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/worker/workers"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

// objects needed to initialize a stateful scheduler
type schedulerDeps struct {
	initialCl       []cluster.Node
	clUpdates       chan []cluster.NodeUpdate
	sc              saga.SagaCoordinator
	rf              func(cluster.Node) runner.Service
	config          SchedulerConfig
	nodeToWorkerMap map[string]runner.Service
}

// returns default scheduler deps populated with in memory fakes
// The default cluster has 5 nodes
func getDefaultSchedDeps() *schedulerDeps {
	tmp, _ := temp.NewTempDir("", "stateful_scheduler_test")
	cl := makeTestCluster("node1", "node2", "node3", "node4", "node5")

	return &schedulerDeps{
		initialCl: cl.nodes,
		clUpdates: cl.ch,
		sc:        sagalogs.MakeInMemorySagaCoordinator(),
		rf: func(n cluster.Node) runner.Service {
			return workers.MakeInmemoryWorker(n, tmp)
		},
		config: SchedulerConfig{
			MaxRetriesPerTask:    0,
			DebugMode:            true,
			RecoverJobsOnStartup: false,
			DefaultTaskTimeout:   time.Second,
		},
	}
}

func makeStatefulSchedulerDeps(deps *schedulerDeps) *statefulScheduler {

	s := NewStatefulScheduler(
		deps.initialCl,
		deps.clUpdates,
		deps.sc,
		deps.rf,
		deps.config,
		stats.NilStatsReceiver(),
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
	jobDef := sched.GenJobDef(1)

	//mock sagalog
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(gomock.Any(), gomock.Any())

	deps := getDefaultSchedDeps()
	deps.sc = saga.MakeSagaCoordinator(sagaLogMock)
	s := makeStatefulSchedulerDeps(deps)

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
}

func Test_StatefulScheduler_ScheduleJobFailure(t *testing.T) {
	jobDef := sched.GenJobDef(1)

	//mock sagalog
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(gomock.Any(), gomock.Any()).Return(errors.New("test error"))

	deps := getDefaultSchedDeps()
	deps.sc = saga.MakeSagaCoordinator(sagaLogMock)
	s := makeStatefulSchedulerDeps(deps)

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
}

func Test_StatefulScheduler_AddJob(t *testing.T) {
	s := makeDefaultStatefulScheduler()
	jobDef := sched.GenJobDef(1)
	go func() {
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()
	id, _ := s.ScheduleJob(jobDef)

	// advance scheduler loop & then verify state
	s.step()
	if len(s.inProgressJobs) != 1 {
		t.Errorf("Expected In Progress Jobs to be 1 not %v", len(s.inProgressJobs))
	}

	if s.getJob(id) == nil {
		t.Errorf("Expected the %v to be an inProgressJobs", id)
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
		return runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), nil, runners.NewNullOutputCreator(), tmp)
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
	mockCtrl := gomock.NewController(t)
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
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage(jobId, taskId, nil))
	sagaLogMock.EXPECT().LogMessage(TaskMessageMatcher{Type: &sagaStartTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}).MaxTimes(1)
	endMessageMatcher := TaskMessageMatcher{JobId: jobId, TaskId: taskId, Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(endMessageMatcher)
	sagaLogMock.EXPECT().LogMessage(saga.MakeEndSagaMessage(jobId))
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
	sc := sagalogs.MakeInMemorySagaCoordinator()
	s, _ := initializeServices(sc, false)

	jobId, taskIds, _ := putJobInScheduler(1, s, true)
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
	sc := sagalogs.MakeInMemorySagaCoordinator()
	s, _ := initializeServices(sc, false)
	putJobInScheduler(1, s, true)

	respCh := sendKillRequest("badJobId", s)

	err := waitForResponse(respCh, s)

	if err == nil {
		t.Errorf("Expected to get job not found error, instead got nil")
	}

	log.Infof("Got job not found error: \n%s", err.Error())

}

func Test_StatefulScheduler_KillFinishedJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinator()
	s, _ := initializeServices(sc, true)
	jobId, taskIds, _ := putJobInScheduler(1, s, false)

	//advance scheduler until the task completes
	for s.getJob(jobId).getTask(taskIds[0]).Status == sched.InProgress {
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

	respCh := sendKillRequest(jobId, s)

	err := waitForResponse(respCh, s)

	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("Expected err to be nil, instead is %v", err.Error())
		}
	} else {
		j := s.getJob(jobId)
		for j == nil {
			s.step()
			j = s.getJob(jobId)
		}
	}

}

func Test_StatefulScheduler_KillNotStartedJob(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinator()
	s, _ := initializeServices(sc, false)

	// create a job with 5 pausing tasks and get them all to InProgress state
	jobId1, _, _ := putJobInScheduler(5, s, true)
	for !allTasksInState("job1", jobId1, s, sched.InProgress) {
		s.step()
	}

	verifyJobStatus("verify started job1", jobId1, sched.InProgress,
		[]sched.Status{sched.InProgress, sched.InProgress, sched.InProgress, sched.InProgress, sched.InProgress}, s, t)

	// put a job with 3 tasks in the queue - all tasks should be in NotStarted state
	jobId2, _, _ := putJobInScheduler(3, s, true)
	verifyJobStatus("verify put job2 in scheduler", jobId2, sched.InProgress,
		[]sched.Status{sched.NotStarted, sched.NotStarted, sched.NotStarted}, s, t)

	// kill the second job
	respCh := sendKillRequest(jobId2, s)

	// pause to let the scheduler pick up the first kill request first.
	time.Sleep(500 * time.Millisecond)

	// kill it a second time to verify the killed 2x error message
	// kill the second job
	respCh2 := sendKillRequest(jobId2, s)
	err := waitForResponse(respCh, s)
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("Expected err to be nil, instead is %v", err.Error())
		}
	} else {
		j := s.getJob(jobId2)
		for j == nil {
			s.step()
			j = s.getJob(jobId2)
		}
	}

	err = waitForResponse(respCh2, s)
	if err == nil {
		t.Error("Killed a job twice, expected an error, got nil")

	} else if !strings.Contains(err.Error(), "was already killed,") {
		t.Errorf("Killed a job twice, expected the error to contain 'was already killed', got %s", err.Error())

	}

	// verify that the first job is still running
	verifyJobStatus("verify job1 still running", jobId1, sched.InProgress,
		[]sched.Status{sched.InProgress, sched.InProgress, sched.InProgress, sched.InProgress, sched.InProgress}, s, t)

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

func initializeServices(sc saga.SagaCoordinator, useDefaultDeps bool) (*statefulScheduler, []*execers.SimExecer) {
	var deps *schedulerDeps
	var exs []*execers.SimExecer
	if useDefaultDeps {
		deps = getDefaultSchedDeps()
	} else {
		deps, exs = getDepsWithPausingWorker()
	}

	deps.sc = sagalogs.MakeInMemorySagaCoordinator()
	return makeStatefulSchedulerDeps(deps), exs
}

// create a job definition containing numTasks tasks and put it in the scheduler.
// usingPausingExecer is true, each task will contain the command "pause"
func putJobInScheduler(numTasks int, s *statefulScheduler, usingPausingExecer bool) (string, []string, error) {
	// create the job and run it to completion
	jobDef := sched.GenJobDef(numTasks)

	var taskIds []string

	if usingPausingExecer {
		//change command to pause.
		for i, _ := range jobDef.Tasks {
			// set the command to pause
			jobDef.Tasks[i].Argv = []string{"pause"}
		}
	}

	for _, task := range jobDef.Tasks {
		taskIds = append(taskIds, task.TaskID)
	}

	// put the job on the jobs channel
	go func() {
		checkJobMsg := <-s.checkJobCh
		checkJobMsg.resultCh <- nil
	}()
	jobId, err := s.ScheduleJob(jobDef)

	// force the first job to pending state without starting it
	s.addJobs()

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

func getDepsWithPausingWorker() (*schedulerDeps, []*execers.SimExecer) {

	tmp, _ := temp.NewTempDir("", "stateful_scheduler_test")
	cl := makeTestCluster("node1", "node2", "node3", "node4", "node5")

	return &schedulerDeps{
		initialCl: cl.nodes,
		clUpdates: cl.ch,
		sc:        sagalogs.MakeInMemorySagaCoordinator(),
		rf: func(n cluster.Node) runner.Service {
			ex := execers.NewSimExecer()
			runner := runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), nil, runners.NewNullOutputCreator(), tmp)
			return runner
		},
		config: SchedulerConfig{
			MaxRetriesPerTask:    0,
			DebugMode:            true,
			RecoverJobsOnStartup: false,
			DefaultTaskTimeout:   time.Second,
		},
	}, nil

}
