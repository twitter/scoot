package scheduler

import (
	//"fmt"
	"github.com/golang/mock/gomock"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	//ci "github.com/scootdev/scoot/sched/clusterimplementations"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	"testing"
)

func Test_RunTask_SuccessfulExecution(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	task := sched.GenTask()

	// ensures node executes task
	nodeMock := cm.NewMockNode(mockCtrl)
	nodeMock.EXPECT().SendMessage(task)

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(saga.MakeEndTaskMessage("job1", "task1", nil))
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)

	s, _ := sagaCoord.MakeSaga("job1", nil)
	runTask(s, nodeMock, "task1", task)

	if !s.GetState().IsTaskStarted("task1") {
		t.Errorf("Expected task to be started")
	}

	if !s.GetState().IsTaskCompleted("task1") {
		t.Errorf("Expected task to be completed")
	}
}

func Test_RunTask_PanicWhenWritingStartTaskReturnsFatalError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	// ensures node does not execute task when a fatal error occurs logging starttask
	nodeMock := cm.NewMockNode(mockCtrl)
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil)).Return(saga.NewInvalidRequestError("test error"))
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)

	s, _ := sagaCoord.MakeSaga("job1", nil)

	defer func() {
		if r := recover(); r != nil {
		}
	}()

	runTask(s, nodeMock, "task1", sched.GenTask())

	t.Errorf("Expected A Fatal Log Returned by SagaLog to cause a Panic")
}

func Test_RunTask_PanicWhenWritingEndTaskReturnsFatalError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	task := sched.GenTask()

	// ensures node executes task
	nodeMock := cm.NewMockNode(mockCtrl)
	nodeMock.EXPECT().SendMessage(task)

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("job1", nil)
	sagaLogMock.EXPECT().LogMessage(saga.MakeStartTaskMessage("job1", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(saga.MakeEndTaskMessage("job1", "task1", nil)).Return(saga.NewInvalidRequestError("test error"))
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)

	s, _ := sagaCoord.MakeSaga("job1", nil)

	defer func() {
		if r := recover(); r != nil {
		}
	}()

	runTask(s, nodeMock, "task1", task)

	t.Errorf("Expected A Fatal Log Returned by SagaLog to cause a Panic")
}
