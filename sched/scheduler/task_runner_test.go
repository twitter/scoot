package scheduler

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/sched/worker/workers"
	"github.com/scootdev/scoot/workerapi"
)

func Test_runTaskAndLog_Successful(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	task := sched.GenTask()

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil))
	endMessageMatcher := TaskMessageMatcher{JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(endMessageMatcher)
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)

	s, _ := sagaCoord.MakeSaga("job1", nil)
	err := runTaskAndLog(s, workers.MakeSimWorker(), "task1", task, false, stats.CurrentStatsReceiver)

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

	err := runTaskAndLog(s, workers.MakeSimWorker(), "task1", task, false, stats.CurrentStatsReceiver)

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
	endMessageMatcher := TaskMessageMatcher{JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(endMessageMatcher).Return(errors.New("test error"))

	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	s, _ := sagaCoord.MakeSaga("job1", nil)

	err := runTaskAndLog(s, workers.MakeSimWorker(), "task1", task, false, stats.CurrentStatsReceiver)

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
	worker := workers.NewPollingWorker(chaos, time.Duration(10)*time.Microsecond)

	chaos.SetError(fmt.Errorf("starting error"))
	err := runTaskAndLog(s, worker, "task1", task, false, stats.CurrentStatsReceiver)

	if err == nil {
		t.Errorf("Expected an error to be returned when Worker RunAndWait returns and error")
	}
}

func Test_runTaskAndLog_MarkFailedTaskAsFinished(t *testing.T) {
	task := sched.GenTask()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	// setup mock worker that returns an error.
	workerMock := worker.NewMockWorker(mockCtrl)
	testErr := errors.New("Test Error, Failed Running Task On Worker")
	retStatus := runner.RunningStatus("run1", "", "")
	workerMock.EXPECT().RunAndWait(task).Return(retStatus, testErr)

	// set up a mock saga log that verifies task is started and completed with a failed task
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil))

	// create expectedProcessStatus that should be looged
	retStatus.Error = testErr.Error()
	retStatus.ExitCode = DeadLetterExitCode
	expectedProcessStatus, _ := workerapi.SerializeProcessStatus(retStatus)
	sagaLogMock.EXPECT().LogMessage(
		saga.MakeEndTaskMessage("job1", "task1", expectedProcessStatus))

	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	s, _ := sagaCoord.MakeSaga("job1", nil)

	err := runTaskAndLog(
		s, workerMock, "task1", task, true, stats.CurrentStatsReceiver)

	if err != nil {
		t.Errorf("Expected error to be nil not, %v", err)
	}
}

type TaskMessageMatcher struct {
	JobId  string
	TaskId string
	Data   gomock.Matcher
}

func (c TaskMessageMatcher) Matches(x interface{}) bool {
	sagaMessage, ok := x.(saga.SagaMessage)

	if !ok {
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
	return "matches to SagaMessage SagaId, TaskId and Data"
}
