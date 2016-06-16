package saga

import (
	"bytes"
	"errors"
	"github.com/golang/mock/gomock"
	"testing"
)

func TestRecoverState_GetMessagesReturnsError(t *testing.T) {

	sagaId := "sagaId"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(nil, errors.New("test error"))
	saga := MakeSaga(sagaLogMock)

	state, err := recoverState(sagaId, saga, RollbackRecovery)

	if err == nil {
		t.Error("Expected GetMessages return error to cause recoverState to return an error")
	}

	if state != nil {
		t.Error("Expected returned SagaState to be nil when Error occurs")
	}
}

func TestRecoverState_GetMessagesReturnsEmptyList(t *testing.T) {
	sagaId := "sagaId"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(nil, nil)
	saga := MakeSaga(sagaLogMock)

	state, err := recoverState(sagaId, saga, RollbackRecovery)

	if state != nil {
		t.Error("Expected returned SagaState to be nil when no messages in SagaLog")
	}

	if err != nil {
		t.Error("Expect Error to be nil when no messages returned from SagaLog")
	}
}

func TestRecoverState_MissingStartMessage(t *testing.T) {
	sagaId := "sagaId"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	msgs := []sagaMessage{
		MakeEndSagaMessage(sagaId),
		MakeStartSagaMessage(sagaId, nil),
	}

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(msgs, nil)
	saga := MakeSaga(sagaLogMock)

	state, err := recoverState(sagaId, saga, RollbackRecovery)

	if err == nil {
		t.Error("Expected error when StartSaga is not first message")
	}

	if state != nil {
		t.Error("Expect sagaState to be nil when error occurs")
	}
}

func TestRecoverState_UpdateSagaStateFails(t *testing.T) {
	sagaId := "sagaId"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	msgs := []sagaMessage{
		MakeStartSagaMessage(sagaId, nil),
		MakeEndTaskMessage(sagaId, "task1", nil),
		MakeEndSagaMessage(sagaId),
	}

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(msgs, nil)
	saga := MakeSaga(sagaLogMock)

	state, err := recoverState(sagaId, saga, RollbackRecovery)

	if err == nil {
		t.Error("Expected error when StartSaga is not first message")
	}

	if state != nil {
		t.Error("Expect sagaState to be nil when error occurs")
	}
}

func TestRecoverState_SuccessfulForwardRecovery(t *testing.T) {
	sagaId := "sagaId"
	taskId := "taskId"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	msgs := []sagaMessage{
		MakeStartSagaMessage(sagaId, []byte{4, 5, 6}),
		MakeStartTaskMessage(sagaId, taskId, []byte{1, 2, 3}),
	}

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(msgs, nil)
	saga := MakeSaga(sagaLogMock)

	state, err := recoverState(sagaId, saga, ForwardRecovery)

	if err != nil {
		t.Errorf("Expected error to be nil %s", err)
	}
	if state == nil {
		t.Error("Expected state to reflect supplied messages")
	}

	if state.SagaId() != sagaId {
		t.Error("Expected SagaState to have same SagaId")
	}

	if !bytes.Equal(state.Job(), []byte{4, 5, 6}) {
		t.Error("Expected SagaState Job to match StartMessage data")
	}

	if !state.IsTaskStarted(taskId) {
		t.Error("Expected SagaState to have task started")
	}

	if !bytes.Equal(state.GetStartTaskData(taskId), []byte{1, 2, 3}) {
		t.Error("Expected SagaState to have data associatd with starttask")
	}

	if state.IsTaskCompleted(taskId) {
		t.Error("Expected SagaState to have task not completed")
	}

	if state.GetEndTaskData(taskId) != nil {
		t.Error("Expected no data associated with end task")
	}

	if state.IsCompTaskStarted(taskId) {
		t.Error("Expected SagaState to have comptask not started")
	}

	if state.GetStartCompTaskData(taskId) != nil {
		t.Error("Expected no data associated with start comp task")
	}

	if state.IsCompTaskCompleted(taskId) {
		t.Error("Expected SagaState to have comptask not completed")
	}

	if state.GetEndCompTaskData(taskId) != nil {
		t.Error("Expected no data associated with end comp task")
	}

	if state.IsSagaCompleted() {
		t.Error("Expected SagaState to not be completed")
	}

	if state.IsSagaAborted() {
		t.Error("Expected SagaState to not be aborted")
	}
}

func TestRecoverState_SuccessfulRollbackRecovery(t *testing.T) {
	sagaId := "sagaId"
	taskId := "taskId"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	msgs := []sagaMessage{
		MakeStartSagaMessage(sagaId, []byte{4, 5, 6}),
		MakeStartTaskMessage(sagaId, taskId, []byte{1, 2, 3}),
	}

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(msgs, nil)
	sagaLogMock.EXPECT().LogMessage(MakeAbortSagaMessage(sagaId))
	saga := MakeSaga(sagaLogMock)

	state, err := recoverState(sagaId, saga, RollbackRecovery)

	if err != nil {
		t.Errorf("Expected error to be nil %s", err)
	}
	if state == nil {
		t.Error("Expected state to reflect supplied messages")
	}

	if !state.IsSagaAborted() {
		t.Error("Expected Saga to be Aborted, not in Safe State")
	}
}

func TestSafeState_AbortedSaga(t *testing.T) {
	state := initializeSagaState()
	state.sagaAborted = true
	state.taskState["task1"] = TaskStarted | CompTaskStarted

	safeState := isSagaInSafeState(state)

	if !safeState {
		t.Error("Expected Aborted Saga to be in Safe State")
	}
}

func TestSafeState_MissingEndTask(t *testing.T) {
	state := initializeSagaState()
	state.taskState["task1"] = TaskStarted
	state.taskState["task2"] = TaskStarted | TaskCompleted

	safeState := isSagaInSafeState(state)

	if safeState {
		t.Error("Expected Saga to be in unsafe state")
	}
}

func TestSafeState_Safe(t *testing.T) {
	state := initializeSagaState()
	state.taskState["task1"] = TaskStarted | TaskCompleted
	state.taskState["task2"] = TaskStarted | TaskCompleted

	safeState := isSagaInSafeState(state)

	if !safeState {
		t.Error("Expected Saga to be in safe state")
	}
}
