package sagalog

import "errors"
import "fmt"
import "testing"
import "github.com/golang/mock/gomock"

import msg "github.com/scootdev/scoot/messages"

func TestStartSaga(t *testing.T) {

	id := "testSaga"
	job := msg.Job{
		Id:      "1",
		Jobtype: "testJob",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(id, job)

	s := saga{
		log: sagaLogMock,
	}

	err := s.StartSaga(id, job)
	if err != nil {
		t.Error(fmt.Sprintf("Expected StartSaga to not return an error"))
	}
}

func TestStartSagaLogError(t *testing.T) {
	id := "testSaga"
	job := msg.Job{
		Id:      "1",
		Jobtype: "testJob",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(id, job).Return(errors.New("Failed to Log StartSaga"))

	s := saga{
		log: sagaLogMock,
	}

	err := s.StartSaga(id, job)

	if err == nil {
		t.Error(fmt.Sprintf("Expected StartSaga to return error if SagaLog fails to log request"))
	}
}

func TestEndSaga(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: EndSaga,
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := saga{
		log: sagaLogMock,
	}

	err := s.EndSaga(entry.sagaId)
	if err != nil {
		t.Error(fmt.Sprintf("Expected EndSaga to not return an error"))
	}
}

func TestEndSagaLogError(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: EndSaga,
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndSaga Message"))

	s := saga{
		log: sagaLogMock,
	}

	err := s.EndSaga(entry.sagaId)
	if err == nil {
		t.Error(fmt.Sprintf("Expected EndSaga to not return an error when write to SagaLog Fails"))
	}
}

func TestAbortSaga(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: AbortSaga,
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := saga{
		log: sagaLogMock,
	}

	err := s.AbortSaga(entry.sagaId)
	if err != nil {
		t.Error(fmt.Sprintf("Expected AbortSaga to not return an error"))
	}
}

func TestAbortSagaLogError(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: AbortSaga,
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log AbortSaga Message"))

	s := saga{
		log: sagaLogMock,
	}

	err := s.AbortSaga(entry.sagaId)
	if err == nil {
		t.Error(fmt.Sprintf("Expected AbortSaga to not return an error when write to SagaLog Fails"))
	}
}

func TestStartTask(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: StartTask,
		taskId:  "task1",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := saga{
		log: sagaLogMock,
	}

	err := s.StartTask(entry.sagaId, entry.taskId)
	if err != nil {
		t.Error(fmt.Sprintf("Expected StartTask to not return an error"))
	}
}

func TestStartTaskLogError(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: StartTask,
		taskId:  "task1",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log StartTask Message"))

	s := saga{
		log: sagaLogMock,
	}

	err := s.StartTask(entry.sagaId, entry.taskId)
	if err == nil {
		t.Error(fmt.Sprintf("Expected StartTask to not return an error when write to SagaLog Fails"))
	}
}

func TestEndTask(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: EndTask,
		taskId:  "task1",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := saga{
		log: sagaLogMock,
	}

	err := s.EndTask(entry.sagaId, entry.taskId)
	if err != nil {
		t.Error(fmt.Sprintf("Expected EndTask to not return an error"))
	}
}

func TestEndTaskLogError(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: EndTask,
		taskId:  "task1",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndTask Message"))

	s := saga{
		log: sagaLogMock,
	}

	err := s.EndTask(entry.sagaId, entry.taskId)
	if err == nil {
		t.Error(fmt.Sprintf("Expected EndTask to not return an error when write to SagaLog Fails"))
	}
}

func TestStartCompTask(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: StartCompTask,
		taskId:  "task1",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := saga{
		log: sagaLogMock,
	}

	err := s.StartCompensatingTask(entry.sagaId, entry.taskId)
	if err != nil {
		t.Error(fmt.Sprintf("Expected StartCompensatingTask to not return an error"))
	}
}

func TestStartCompTaskLogError(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: StartCompTask,
		taskId:  "task1",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log StartCompTask Message"))

	s := saga{
		log: sagaLogMock,
	}

	err := s.StartCompensatingTask(entry.sagaId, entry.taskId)
	if err == nil {
		t.Error(fmt.Sprintf("Expected StartCompTask to not return an error when write to SagaLog Fails"))
	}
}

func TestEndCompTask(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: EndCompTask,
		taskId:  "task1",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := saga{
		log: sagaLogMock,
	}

	err := s.EndCompensatingTask(entry.sagaId, entry.taskId)
	if err != nil {
		t.Error(fmt.Sprintf("Expected EndCompensatingTask to not return an error"))
	}
}

func TestEndCompTaskLogError(t *testing.T) {
	entry := SagaMessage{
		sagaId:  "1",
		msgType: EndCompTask,
		taskId:  "task1",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndCompTask Message"))

	s := saga{
		log: sagaLogMock,
	}

	err := s.EndCompensatingTask(entry.sagaId, entry.taskId)
	if err == nil {
		t.Error(fmt.Sprintf("Expected EndCompTask to not return an error when write to SagaLog Fails"))
	}
}

func TestGetSagaState(t *testing.T) {
	job := msg.Job{
		Id: "test1",
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetSagaState("1").Return(SagaStateFactory("1", job), nil)

	s := saga{
		log: sagaLogMock,
	}

	_, err := s.GetSagaState("1")
	if err != nil {
		t.Error(fmt.Sprintf("Expected GetSagaState to not return an erorr"))
	}
}
