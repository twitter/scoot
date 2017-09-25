package scheduler

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	runnermock "github.com/twitter/scoot/runner/mocks"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/sched/worker/workers"
	"github.com/twitter/scoot/workerapi"
)

var tmp *temp.TempDir

func TestMain(m *testing.M) {
	log.AddHook(hooks.NewContextHook())
	tmp, _ = temp.NewTempDir("", "task_runner_test")
	os.Exit(m.Run())
}

func get_testTaskRunner(s *saga.Saga, r runner.Service, jobId, taskId string,
	task sched.TaskDefinition, markCompleteOnFailure bool, stat stats.StatsReceiver) *taskRunner {
	return &taskRunner{
		saga:   s,
		runner: r,
		stat:   stat,

		markCompleteOnFailure: markCompleteOnFailure,
		defaultTaskTimeout:    30 * time.Second,
		taskTimeoutOverhead:   1 * time.Second,
		runnerRetryTimeout:    0,
		runnerRetryInterval:   0,

		task: task,
		LogTags: tags.LogTags{
			JobID:  jobId,
			TaskID: taskId,
		},
		nodeSt: &nodeState{node: cluster.NewIdNode(jobId + "." + taskId)},
	}
}

func Test_runTaskAndLog_Successful(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	task := sched.GenTask()

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(TaskMessageMatcher{Type: &sagaStartTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}).MaxTimes(1)
	endMessageMatcher := TaskMessageMatcher{Type: &sagaEndTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(endMessageMatcher)
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)

	s, _ := sagaCoord.MakeSaga("job1", nil)
	err := get_testTaskRunner(s, workers.MakeDoneWorker(tmp), "job1", "task1", task, false, stats.NilStatsReceiver()).run()
	if err != nil {
		t.Errorf("Unexpected Error %v", err)
	}
}

func Test_runTaskAndLog_IncludeRunningStatus(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	task := sched.TaskDefinition{Command: runner.Command{
		Argv: []string{"sleep 500", "complete 0"},
	},
	}

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil))
	// Make sure that we include another start task message
	sagaLogMock.EXPECT().LogMessage(TaskMessageMatcher{Type: &sagaStartTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()})
	endMessageMatcher := TaskMessageMatcher{Type: &sagaEndTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(endMessageMatcher)
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)

	s, _ := sagaCoord.MakeSaga("job1", nil)
	err := get_testTaskRunner(s, workers.MakeSimWorker(tmp), "job1", "task1", task, false, stats.NilStatsReceiver()).run()
	if err != nil {
		t.Errorf("Unexpected Error %v", err)
	}

}

func Test_runTaskAndLog_FailedToLogStartTask(t *testing.T) {
	task := sched.GenTask()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil)).Return(errors.New("test error"))
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	s, _ := sagaCoord.MakeSaga("job1", nil)

	err := get_testTaskRunner(s, workers.MakeDoneWorker(tmp), "job1", "task1", task, false, stats.NilStatsReceiver()).run()

	if err == nil {
		t.Errorf("Expected an error to be returned if Logging StartTask Fails")
	}
}

func Test_runTaskAndLog_FailedToLogEndTask(t *testing.T) {
	task := sched.GenTask()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(TaskMessageMatcher{Type: &sagaStartTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}).MaxTimes(1)
	endMessageMatcher := TaskMessageMatcher{Type: &sagaEndTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(endMessageMatcher).Return(errors.New("test error"))

	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	s, _ := sagaCoord.MakeSaga("job1", nil)

	err := get_testTaskRunner(s, workers.MakeDoneWorker(tmp), "job1", "task1", task, false, stats.NilStatsReceiver()).run()

	if err == nil {
		t.Errorf("Expected an error to be returned if Logging EndTask Fails")
	}
}

func Test_runTaskAndLog_TaskFailsToRun(t *testing.T) {
	task := sched.GenTask()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil))
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	s, _ := sagaCoord.MakeSaga("job1", nil)

	chaos := runners.NewChaosRunner(nil)

	chaos.SetError(fmt.Errorf("starting error"))
	err := get_testTaskRunner(s, chaos, "job1", "task1", task, false, stats.NilStatsReceiver()).run()

	if err == nil {
		t.Errorf("Expected an error to be returned when Worker RunAndWait returns and error")
	}
}

func Test_runTaskAndLog_MarkFailedTaskAsFinished(t *testing.T) {
	task := sched.GenTask()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	testErr := errors.New("Test Error, Failed Running Task On Worker")
	// setup mock worker that returns an error.
	chaos := runners.NewChaosRunner(nil)

	chaos.SetError(testErr)

	// set up a mock saga log that verifies task is started and completed with a failed task
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	var retStatus runner.RunStatus

	retStatus.State = runner.FAILED
	retStatus.Error = emptyStatusError("job1", "task1", testErr) + DeadLetterTrailer
	expectedProcessStatus, _ := workerapi.SerializeProcessStatus(retStatus)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(saga.MakeEndTaskMessage("job1", "task1", expectedProcessStatus))

	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	s, _ := sagaCoord.MakeSaga("job1", nil)

	err := get_testTaskRunner(s, chaos, "job1", "task1", task, true, stats.NilStatsReceiver()).run()

	if err.(*taskError).runnerErr == nil {
		t.Errorf("Expected result error to not be nil, got: %v", err)
	}
}

func Test_runTaskWithFailedStartTask(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", gomock.Any())
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	s, _ := sagaCoord.MakeSaga("job1", nil)

	// StartTask err should result in the appropriate taskError below.
	msgMatcher := TaskMessageMatcher{Type: &sagaStartTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	startTaskErr := errors.New("StartTaskErr")
	sagaLogMock.EXPECT().LogMessage(msgMatcher).Return(startTaskErr)

	runMock := runnermock.NewMockService(mockCtrl)
	err := get_testTaskRunner(s, runMock, "job1", "task1", sched.GenTask(), true, stats.NilStatsReceiver()).run()
	if err == nil {
		t.Errorf("Expected error to be non-nil")
	} else if terr, ok := err.(*taskError); !ok {
		t.Errorf("Expected error to be a *taskError, was: %v", err)
	} else if terr.sagaErr != startTaskErr {
		t.Errorf("Expected saga error: %v, got: %v", startTaskErr, terr.sagaErr)
	}
}

func Test_runTaskWithRunRetry(t *testing.T) {
	statsRegistry := stats.NewFinagleStatsRegistry()
	statsReceiver, _ := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsRegistry }, 0)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", gomock.Any())
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	s, _ := sagaCoord.MakeSaga("job1", nil)

	msgMatcher := TaskMessageMatcher{Type: &sagaStartTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(msgMatcher)
	msgMatcher = TaskMessageMatcher{Type: &sagaEndTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(msgMatcher)

	runMock := runnermock.NewMockService(mockCtrl)
	runErr := errors.New("RunErr")
	runMock.EXPECT().Run(gomock.Any()).Return(runner.RunStatus{}, runErr).Times(2)

	tr := get_testTaskRunner(s, runMock, "job1", "task1", sched.GenTask(), true, statsReceiver)
	tr.runnerRetryTimeout = 3 * time.Millisecond
	tr.runnerRetryInterval = 2 * time.Millisecond
	err := tr.run()

	if err == nil {
		t.Errorf("Expected error to be non-nil")
	} else if terr, ok := err.(*taskError); !ok {
		t.Errorf("Expected error to be a *taskError, was: %v", err)
	} else if terr.runnerErr != runErr {
		t.Errorf("Expected saga error: %v, got: %v", runErr, terr.runnerErr)
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			stats.SchedTaskStartRetries: {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}
}

func Test_runTaskWithQueryRetry(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", gomock.Any())
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	s, _ := sagaCoord.MakeSaga("job1", nil)

	msgMatcher := TaskMessageMatcher{Type: &sagaStartTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(msgMatcher)
	msgMatcher = TaskMessageMatcher{Type: &sagaEndTask, JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(msgMatcher)

	runMock := runnermock.NewMockService(mockCtrl)
	queryErr := errors.New("QueryErr")
	runMock.EXPECT().Run(gomock.Any()).Return(runner.RunStatus{State: runner.PENDING}, nil)
	runMock.EXPECT().Query(gomock.Any(), gomock.Any()).Return(
		[]runner.RunStatus{{}}, runner.ServiceStatus{}, queryErr).Times(2)
	runMock.EXPECT().Abort(gomock.Any())

	tr := get_testTaskRunner(s, runMock, "job1", "task1", sched.GenTask(), true, stats.NilStatsReceiver())
	tr.runnerRetryTimeout = 3 * time.Millisecond
	tr.runnerRetryInterval = 2 * time.Millisecond
	err := tr.run()

	if err == nil {
		t.Errorf("Expected error to be non-nil")
	} else if terr, ok := err.(*taskError); !ok {
		t.Errorf("Expected error to be a *taskError, was: %v", err)
	} else if terr.runnerErr != queryErr {
		t.Errorf("Expected saga error: %v, got: %v", queryErr, terr.runnerErr)
	}
}

var sagaStartTask = saga.StartTask
var sagaEndTask = saga.EndTask

type TaskMessageMatcher struct {
	Type   *saga.SagaMessageType
	JobId  string
	TaskId string
	Data   gomock.Matcher
}

func (c TaskMessageMatcher) Matches(x interface{}) bool {
	sagaMessage, ok := x.(saga.SagaMessage)

	if !ok {
		return false
	}

	if c.Type != nil && *c.Type != sagaMessage.MsgType {
		return false
	}

	if c.JobId != sagaMessage.SagaId {
		return false
	}

	if c.TaskId != sagaMessage.TaskId {
		return false
	}

	if !c.Data.Matches(sagaMessage.Data) {
		return false
	}

	return true
}

func (c TaskMessageMatcher) String() string {
	if c.Type != nil {
		return fmt.Sprintf("{%v %v %v %v}", *c.Type, c.JobId, c.TaskId, c.Data)
	}
	return fmt.Sprintf("{%v %v %v}", c.JobId, c.TaskId, c.Data)
}
