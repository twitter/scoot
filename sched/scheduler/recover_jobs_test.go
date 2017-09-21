package scheduler

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched"
	"testing"
	"time"
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
		slog.EXPECT().GetActiveSagas().Return(nil, errors.New("Test Error Again!")),
		slog.EXPECT().GetActiveSagas().Return(nil, errors.New("Omg so many Test Errors!")),
		slog.EXPECT().GetActiveSagas().Return([]string{}, nil),
	)

	// Expect no messages added to addJobCh
	addJobCh := make(chan jobAddedMsg, 1)
	recoverJobs(sc, addJobCh)

	// Nothing to verify just ensuring that recoverJobs eventually succeeds
}

func Test_RecoverJobs_ActiveSagas(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	job1 := sched.GenJob("saga1", 10)
	job2 := sched.GenJob("saga2", 5)
	job1Data, _ := (&job1).Serialize()
	job2Data, _ := (&job2).Serialize()

	slog.EXPECT().GetActiveSagas().Return([]string{"saga1", "saga2"}, nil)
	slog.EXPECT().GetMessages("saga1").Return([]saga.SagaMessage{
		saga.MakeStartSagaMessage("saga1", job1Data),
	}, nil)
	slog.EXPECT().GetMessages("saga2").Return([]saga.SagaMessage{
		saga.MakeStartSagaMessage("saga2", job2Data),
	}, nil)

	addJobCh := make(chan jobAddedMsg, 5)
	recoverJobs(sc, addJobCh)
	recoveredJobs := make(map[string]jobAddedMsg)

	for i := 0; i < 2; i++ {
		select {
		case msg := <-addJobCh:
			recoveredJobs[msg.saga.GetState().SagaId()] = msg
		default:
		}
	}

	if _, ok1 := recoveredJobs["saga1"]; !ok1 {
		t.Errorf("Expected saga1 to be rescheduled")
	}

	if _, ok2 := recoveredJobs["saga2"]; !ok2 {
		t.Errorf("expected saga2 to be rescheduled")
	}
}

// verifies that if recovering saga returns nil, then we don't add it
// to the addJobs channel
func Test_RecoverJob_NilActiveSaga(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	job := sched.GenJob("sagaId", 10)
	jobData, _ := (&job).Serialize()

	slog.EXPECT().GetActiveSagas().Return([]string{"saga1", "saga2"}, nil)
	slog.EXPECT().GetMessages("saga1").Return([]saga.SagaMessage{
		saga.MakeStartSagaMessage("saga1", jobData),
	}, nil)
	slog.EXPECT().GetMessages("saga2").Return(nil, nil)

	addJobCh := make(chan jobAddedMsg, 5)
	recoverJobs(sc, addJobCh)
	recoveredJobs := make(map[string]jobAddedMsg)

	for i := 0; i < 2; i++ {
		select {
		case msg := <-addJobCh:
			recoveredJobs[msg.saga.GetState().SagaId()] = msg
		default:
		}
	}

	if _, ok1 := recoveredJobs["saga1"]; !ok1 {
		t.Errorf("Expected saga1 to be rescheduled")
	}

	if _, ok2 := recoveredJobs["saga2"]; ok2 {
		t.Errorf("expected saga2 to be not be rescheduled")
	}
}

// verifies that a non deserializable job stored in the saga
// log is skipped over.  This would only happen if we introduced
// a breaking change or there was data corruption
func Test_RecoverJob_NonDeSerializableJob(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	slog.EXPECT().GetActiveSagas().Return([]string{"saga1"}, nil)
	slog.EXPECT().GetMessages("saga1").Return([]saga.SagaMessage{
		saga.MakeStartSagaMessage("saga1", []byte{0, 1, 2, 3, 4}),
	}, nil)

	addJobCh := make(chan jobAddedMsg, 5)
	recoverJobs(sc, addJobCh)

	select {
	case msg := <-addJobCh:
		t.Errorf("Expected no job to be added when job cannot be deserialized, Actual: %+v", msg)
	default:
	}
}

func Test_RecoverSaga_ActiveSaga(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	job := sched.GenJob("sagaId", 10)
	jobData, _ := (&job).Serialize()
	slog.EXPECT().GetMessages("saga1").Return([]saga.SagaMessage{
		saga.MakeStartSagaMessage("saga1", jobData),
	}, nil)

	s := recoverSaga(sc, "saga1")
	if s == nil {
		t.Errorf("Expeceted in progress saga to be returned not nil")
	}
}

func Test_RecoverSaga_NonExistantSaga(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	slog.EXPECT().GetMessages("saga1").Return(nil, nil)

	s := recoverSaga(sc, "saga1")
	if s != nil {
		t.Errorf("expected nil saga to be returned when saga is not in the log. Actual: %+v", s)
	}
}

func Test_RecoverSaga_CompletedActiveSaga(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc, slog := makeMockSagaCoord(mockCtrl)
	slog.EXPECT().GetMessages("saga1").Return([]saga.SagaMessage{
		saga.MakeStartSagaMessage("saga1", nil),
		saga.MakeStartTaskMessage("saga1", "task1", nil),
		saga.MakeEndTaskMessage("saga1", "task1", nil),
		saga.MakeEndSagaMessage("saga1"),
	}, nil)

	s := recoverSaga(sc, "saga1")

	if s != nil {
		t.Errorf("expected nil saga to be returned when saga is completed. Actual: %+v", s)
	}
}

func Test_RecoverSaga_FatalError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sc, slog := makeMockSagaCoord(mockCtrl)
	// have sagalog return a non well formed saga - unrecoverable error
	slog.EXPECT().GetMessages("saga1").Return([]saga.SagaMessage{
		saga.MakeStartTaskMessage("saga1", "task1", nil),
		saga.MakeEndTaskMessage("saga1", "task1", nil),
		saga.MakeEndSagaMessage("saga1"),
	}, nil)

	s := recoverSaga(sc, "saga1")
	if s != nil {
		t.Errorf("expected returned saga to be nil, when unrecoverable error occurs, Actual: %+v", s)
	}
}

func Test_RecoverSaga_RetryableError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sc, slog := makeMockSagaCoord(mockCtrl)
	slog.EXPECT().GetMessages("saga1").Return(nil, saga.NewInternalLogError("test saga log temporarily unavailable"))
	slog.EXPECT().GetMessages("saga1").Return([]saga.SagaMessage{
		saga.MakeStartSagaMessage("saga1", nil),
	}, nil)

	s := recoverSaga(sc, "saga1")
	if s == nil {
		t.Errorf("expected saga to be not nil, saga recovery should retry")
	}
}

func Test_CalculateExponentialBackoff_LessThanMax(t *testing.T) {
	delay := calculateExponentialBackoff(1, time.Duration(1)*time.Second)
	if delay != time.Duration(500)*time.Millisecond {
		t.Errorf("expected backoff to be 500 milliseconds not %v", delay)
	}
}

func Test_CalculateExponentialBackoff_GreaterThanMax(t *testing.T) {
	maxDelay := time.Duration(250) * time.Millisecond
	delay := calculateExponentialBackoff(1, maxDelay)
	if delay != maxDelay {
		t.Errorf("expected backoff to be %v, milliseconds not %v", maxDelay, delay)
	}
}
