package saga

import "errors"
import "fmt"
import "testing"
import "github.com/golang/mock/gomock"

func TestStartSaga(t *testing.T) {

	id := "testSaga"
	var job []byte

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(id, job)

	s := Saga{
		log: sagaLogMock,
	}

	err := s.StartSaga(id, job)
	if err != nil {
		t.Error(fmt.Sprintf("Expected StartSaga to not return an error"))
	}
}

func TestStartSagaLogError(t *testing.T) {
	id := "testSaga"
	var job []byte

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(id, job).Return(errors.New("Failed to Log StartSaga"))

	s := Saga{
		log: sagaLogMock,
	}

	err := s.StartSaga(id, job)

	if err == nil {
		t.Error(fmt.Sprintf("Expected StartSaga to return error if SagaLog fails to log request"))
	}
}

func TestEndSaga(t *testing.T) {
	entry := EndSagaMessageFactory("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := Saga{
		log: sagaLogMock,
	}

	err := s.EndSaga(entry.sagaId)
	if err != nil {
		t.Error(fmt.Sprintf("Expected EndSaga to not return an error"))
	}
}

func TestEndSagaLogError(t *testing.T) {
	entry := EndSagaMessageFactory("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndSaga Message"))

	s := Saga{
		log: sagaLogMock,
	}

	err := s.EndSaga(entry.sagaId)
	if err == nil {
		t.Error(fmt.Sprintf("Expected EndSaga to not return an error when write to SagaLog Fails"))
	}
}

func TestAbortSaga(t *testing.T) {
	entry := AbortSagaMessageFactory("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := Saga{
		log: sagaLogMock,
	}

	err := s.AbortSaga(entry.sagaId)
	if err != nil {
		t.Error(fmt.Sprintf("Expected AbortSaga to not return an error"))
	}
}

func TestAbortSagaLogError(t *testing.T) {
	entry := AbortSagaMessageFactory("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log AbortSaga Message"))

	s := Saga{
		log: sagaLogMock,
	}

	err := s.AbortSaga("testSaga")
	if err == nil {
		t.Error(fmt.Sprintf("Expected AbortSaga to not return an error when write to SagaLog Fails"))
	}
}

func TestStartTask(t *testing.T) {
	entry := StartTaskMessageFactory("testSaga", "task1")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := Saga{
		log: sagaLogMock,
	}

	err := s.StartTask("testSaga", "task1")
	if err != nil {
		t.Error(fmt.Sprintf("Expected StartTask to not return an error"))
	}
}

func TestStartTaskLogError(t *testing.T) {
	entry := StartTaskMessageFactory("testSaga", "task1")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log StartTask Message"))

	s := Saga{
		log: sagaLogMock,
	}

	err := s.StartTask("testSaga", "task1")
	if err == nil {
		t.Error(fmt.Sprintf("Expected StartTask to not return an error when write to SagaLog Fails"))
	}
}

func TestEndTask(t *testing.T) {
	entry := EndTaskMessageFactory("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := Saga{
		log: sagaLogMock,
	}

	err := s.EndTask("testSaga", "task1", nil)
	if err != nil {
		t.Error(fmt.Sprintf("Expected EndTask to not return an error"))
	}
}

func TestEndTaskLogError(t *testing.T) {
	entry := EndTaskMessageFactory("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndTask Message"))

	s := Saga{
		log: sagaLogMock,
	}

	err := s.EndTask("testSaga", "task1", nil)
	if err == nil {
		t.Error(fmt.Sprintf("Expected EndTask to not return an error when write to SagaLog Fails"))
	}
}

func TestStartCompTask(t *testing.T) {
	entry := StartCompTaskMessageFactory("sagaId", "task1")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := Saga{
		log: sagaLogMock,
	}

	err := s.StartCompensatingTask("sagaId", "task1")
	if err != nil {
		t.Error(fmt.Sprintf("Expected StartCompensatingTask to not return an error"))
	}
}

func TestStartCompTaskLogError(t *testing.T) {
	entry := StartCompTaskMessageFactory("sagaId", "task1")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log StartCompTask Message"))

	s := Saga{
		log: sagaLogMock,
	}

	err := s.StartCompensatingTask("sagaId", "task1")
	if err == nil {
		t.Error(fmt.Sprintf("Expected StartCompTask to not return an error when write to SagaLog Fails"))
	}
}

func TestEndCompTask(t *testing.T) {
	entry := EndCompTaskMessageFactory("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := Saga{
		log: sagaLogMock,
	}

	err := s.EndCompensatingTask("testSaga", "task1", nil)
	if err != nil {
		t.Error(fmt.Sprintf("Expected EndCompensatingTask to not return an error"))
	}
}

func TestEndCompTaskLogError(t *testing.T) {
	entry := EndCompTaskMessageFactory("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndCompTask Message"))

	s := Saga{
		log: sagaLogMock,
	}

	err := s.EndCompensatingTask("testSaga", "task1", nil)
	if err == nil {
		t.Error(fmt.Sprintf("Expected EndCompTask to not return an error when write to SagaLog Fails"))
	}
}

func TestGetSagaState(t *testing.T) {
	var job []byte

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaState, _ := SagaStateFactory("1", job)
	sagaLogMock.EXPECT().GetSagaState("1").Return(sagaState, nil)

	s := Saga{
		log: sagaLogMock,
	}

	_, err := s.GetSagaState("1")
	if err != nil {
		t.Error(fmt.Sprintf("Expected GetSagaState to not return an erorr"))
	}
}
