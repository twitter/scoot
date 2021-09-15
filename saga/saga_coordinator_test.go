package saga

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
)

func TestMakeSaga(t *testing.T) {
	id := "testSaga"
	var job []byte

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(id, job)

	sc := MakeSagaCoordinator(sagaLogMock, nil)
	saga, err := sc.MakeSaga(id, job)

	if err != nil {
		t.Error("Expected newSaga to not return an error")
	}
	if saga.id != id {
		t.Error("Expected state.SagaId to equal 'testSaga'")
	}
}

func TestMakeSagaLogError(t *testing.T) {
	id := "testSaga"
	var job []byte

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(id, job).Return(errors.New("Failed to Log StartSaga"))

	sc := MakeSagaCoordinator(sagaLogMock, nil)
	saga, err := sc.MakeSaga(id, job)

	if err == nil {
		t.Error("Expected StartSaga to return error if SagaLog fails to log request")
	}
	if saga != nil {
		t.Error("Expected returned state to be nil when error occurs")
	}
}

func TestStartup_ReturnsError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetActiveSagas().Return(nil, errors.New("test error"))

	sc := MakeSagaCoordinator(sagaLogMock, nil)
	ids, err := sc.Startup()

	if err == nil {
		t.Error("Expected error to not be nil")
	}
	if ids != nil {
		t.Error("ids should be null when error is returned")
	}
}

func TestStartup_ReturnsIds(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetActiveSagas().Return([]string{"saga1", "saga2", "saga3"}, nil)

	sc := MakeSagaCoordinator(sagaLogMock, nil)
	ids, err := sc.Startup()

	if err != nil {
		t.Error("unexpected error returned ", err)
	}
	if ids == nil {
		t.Error("expected is to be returned")
	}

	expectedIds := make(map[string]bool)
	expectedIds["saga1"] = true
	expectedIds["saga2"] = true
	expectedIds["saga3"] = true

	for _, id := range ids {
		if !expectedIds[id] {
			t.Error("unexpectedId returend ", id)
		}
	}
}

func TestRecoverSagaState(t *testing.T) {
	sagaId := "saga1"
	taskId := "task1"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	msgs := []SagaMessage{
		MakeStartSagaMessage(sagaId, nil),
		MakeStartTaskMessage(sagaId, taskId, nil),
		MakeEndTaskMessage(sagaId, taskId, nil),
		MakeEndSagaMessage(sagaId),
	}

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(msgs, nil)

	sc := MakeSagaCoordinator(sagaLogMock, nil)
	saga, err := sc.RecoverSagaState(sagaId, ForwardRecovery)

	if err != nil {
		t.Error("unexpected error returned ", err)
	}
	if saga == nil {
		t.Error("expected returned state to not be nil")
	}

	if !saga.state.IsSagaCompleted() {
		t.Error("expected returned saga state to be completed saga")
	}
}

func TestRecoverSagaState_NoMessages(t *testing.T) {
	sagaId := "saga1"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(nil, nil)

	sc := MakeSagaCoordinator(sagaLogMock, nil)
	saga, err := sc.RecoverSagaState(sagaId, ForwardRecovery)

	if err != nil {
		t.Error("unexpected error returned", err)
	}

	if saga != nil {
		t.Errorf("expected returned saga to be nil not %+v", saga)
	}
}

func TestRecoverSagaState_ReturnsError(t *testing.T) {
	sagaId := "saga1"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(nil, errors.New("test error"))

	sc := MakeSagaCoordinator(sagaLogMock, nil)
	saga, err := sc.RecoverSagaState(sagaId, RollbackRecovery)

	if err == nil {
		t.Error("expeceted error to not be nil")
	}

	if saga != nil {
		t.Error("expected returned state to be nil when error occurs")
	}
}
