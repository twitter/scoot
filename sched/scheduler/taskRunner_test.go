package scheduler

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/worker/workers"
)


func Test_runTaskAndLog_Successful(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	task := sched.GenTask()

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil))
	endMessageMatcher := taskMessageMatcher{JobId: "job1", TaskId: "task1", Data: gomock.Any()}
	sagaLogMock.EXPECT().LogMessage(endMessageMatcher)
	//sagaLogMock.EXPECT().LogMessage(saga.MakeEndTaskMessage("job1", "task1", nil))
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)

	s, _ := sagaCoord.MakeSaga("job1", nil)
	err := runTaskAndLog(s, workers.MakeSimWorker(), "task1", task)

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

	err := runTaskAndLog(s, workers.MakeSimWorker(), "task1", task)

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
	sagaLogMock.EXPECT().LogMessage(saga.MakeEndTaskMessage("job1", "task1", nil)).Return(errors.New("test error"))
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	s, _ := sagaCoord.MakeSaga("job1", nil)

	err := runTaskAndLog(s, workers.MakeSimWorker(), "task1", task)

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
	err := runTaskAndLog(s, worker, "task1", task)

	if err == nil {
		t.Errorf("Expected an error to be returned when Worker RunAndWait returns and error")
	}
}



type taskMessageMatcher struct {
	JobId  string
	TaskId string
	Data   gomock.Matcher
}

func (c taskMessageMatcher) Matches(x interface{}) bool {
	fmt.Printf("******* in end message matcher\n")
	sagaMessage, ok := x.(saga.SagaMessage)

	if !ok {
		fmt.Printf("****** not ok\n")
		return false
	}

	if c.JobId != sagaMessage.SagaId {
		fmt.Printf(fmt.Sprintf("****** jobid: %s, %s\n", c.JobId, sagaMessage.SagaId))
		return false
	}

	if c.TaskId != sagaMessage.TaskId {
		fmt.Printf(fmt.Sprintf("****** taskid: %s, %s\n", c.TaskId, sagaMessage.TaskId))
		return false
	}

	//if !c.Data.Matches(sagaMessage.Data) {
	//	fmt.Printf("****** not any match\n")
	//	return false
	//}

	return true
}
func (c taskMessageMatcher) String() string {
	return "matches to SagaMessage SagaId, TaskId and Data"
}

