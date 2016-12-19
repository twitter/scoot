package scheduler

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"testing"
)

func makeMockSagaCoord(mockCtrl *gomock.Controller) (saga.SagaCoordinator, *saga.MockSagaLog) {
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)
	return sagaCoord, sagaLogMock
}

// Call to GetActiveSagas returns nil
func Test_RecoverJobs_NilActiveSagas(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	slog.EXPECT().GetActiveSagas().Return(nil, nil)

	// Expect no messages added to addJobCh
	addJobCh := make(chan jobAddedMsg, 1)
	recoverJobs(sc, addJobCh)

	select {
	case msg := <-addJobCh:
		t.Errorf("unexpected message added to addJobCh %v", msg)
	default:
	}
}

// Call to GetActiveSagas returns empty list
func Test_RecoverJobs_EmptyActiveSagas(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	slog.EXPECT().GetActiveSagas().Return([]string{}, nil)

	// Expect no messages added to addJobCh
	addJobCh := make(chan jobAddedMsg, 1)
	recoverJobs(sc, addJobCh)

	select {
	case msg := <-addJobCh:
		t.Errorf("unexpected message added to addJobCh %v", msg)
	default:
	}
}

// Ensure it will retry if GetActiveSagasFails
func Test_RecoverJobs_GetActiveSagasFails(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	gomock.InOrder(
		slog.EXPECT().GetActiveSagas().Return(nil, errors.New("Test Error!")),
		//slog.EXPECT().GetActiveSagas().Return(nil, errors.New("Test Error Again!")),
		//slog.EXPECT().GetActiveSagas().Return(nil, errors.New("Omg so many Test Errors!")),
		slog.EXPECT().GetActiveSagas().Return([]string{}, nil),
	)

	// Expect no messages added to addJobCh
	addJobCh := make(chan jobAddedMsg, 1)
	recoverJobs(sc, addJobCh)

	// Nothing to verify just ensuring that recoverJobs eventually succeeds
}

func Test_RecoverJobs_ActiveSaga(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	job := sched.GenJob("sagaId", 10)
	jobData, _ := (&job).Serialize()
	slog.EXPECT().GetActiveSagas().Return([]string{"saga1"}, nil)
	slog.EXPECT().GetMessages("saga1").Return([]saga.SagaMessage{
		saga.MakeStartSagaMessage("saga1", jobData),
	}, nil)

	addJobCh := make(chan jobAddedMsg, 1)
	recoverJobs(sc, addJobCh)

	select {
	case msg := <-addJobCh:
		if msg.saga.GetState().SagaId() != "saga1" {
			t.Errorf("Unexpected Add Job Message %v", msg)
		}
	default:
		t.Errorf("Expected one message to be added to addJobCh")
	}
}

func Test_RecoverJobs_NonExistantSaga(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	slog.EXPECT().GetActiveSagas().Return([]string{"saga1"}, nil)
	slog.EXPECT().GetMessages("saga1").Return(nil, nil)

	addJobCh := make(chan jobAddedMsg, 1)
	recoverJobs(sc, addJobCh)

	select {
	case msg := <-addJobCh:
		t.Errorf("unexpected message added to addJobCh %v", msg)
	default:
	}
}

func Test_RecoverJobs_CompletedActiveSaga(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	slog.EXPECT().GetActiveSagas().Return([]string{"saga1"}, nil)
	slog.EXPECT().GetMessages("saga1").Return([]saga.SagaMessage{
		saga.MakeStartSagaMessage("saga1", nil),
		saga.MakeStartTaskMessage("saga1", "task1", nil),
		saga.MakeEndTaskMessage("saga1", "task1", nil),
		saga.MakeEndSagaMessage("saga1"),
	}, nil)

	addJobCh := make(chan jobAddedMsg, 1)
	recoverJobs(sc, addJobCh)

	select {
	case msg := <-addJobCh:
		t.Errorf("unexpected message added to addJobCh %v", msg)
	default:
	}
}
