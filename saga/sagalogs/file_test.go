package sagalogs

import (
	"bytes"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/twitter/scoot/saga"
)

func getDirName() string {
	return path.Join(os.TempDir(), "sagas")
}

// Removes the sagas directory and all files created during
// a test execution
func testCleanup(t *testing.T) {
	err := os.RemoveAll(getDirName())
	if err != nil {
		t.Errorf("error cleaning up saga directory, %v", err)
	}
}

// returns true if its in the active saga list, false otherwise
func isSagaInActiveList(sagaId string, slog saga.SagaLog) bool {
	// check its in active sagas
	activeSagas, _ := slog.GetActiveSagas()

	for _, activeSagaId := range activeSagas {
		if activeSagaId == sagaId {
			return true
		}
	}

	return false
}

func TestStartSaga(t *testing.T) {
	defer testCleanup(t)

	dirName := getDirName()
	sagaId := "saga1"

	slog, err := MakeFileSagaLog(dirName)
	if err != nil {
		t.Errorf("Unexpected Error Returned %v", err)
	}

	data := []byte{0, 1, 2, 3, 4, 5}
	err = slog.StartSaga(sagaId, data)
	if err != nil {
		t.Errorf("Unexpected Error starting Saga")
	}

	msgs, err := slog.GetMessages(sagaId)
	if err != nil {
		t.Errorf("Unexpected Error Getting Messages %v", err)
	}

	if len(msgs) != 1 {
		t.Errorf("Expected 1 message for saga, not %v, Messages: %+v", len(msgs), msgs)
	}

	if msgs[0].MsgType != saga.StartSaga {
		t.Errorf("Unexpected Message Type.  Expected: StartSaga, Actual: %v", msgs[0].MsgType)
	}

	if !bytes.Equal(data, msgs[0].Data) {
		t.Errorf("Expected Start Saga Message Data to be: %v, Actual: %v", data, msgs[0].Data)
	}

	if !isSagaInActiveList(sagaId, slog) {
		t.Errorf("Expected Saga to be in active list")
	}
}

func TestStartSaga_SagaFileAlreadyExists(t *testing.T) {
	defer testCleanup(t)

	dirName := getDirName()
	sagaId := "start_saga_called_twice"
	data1 := []byte{0, 1, 2, 3, 4, 5}
	data2 := []byte{6, 7, 8, 9}

	slog, _ := MakeFileSagaLog(dirName)
	slog.StartSaga(sagaId, data1)
	err := slog.StartSaga(sagaId, data2)
	if err != nil {
		t.Errorf("Unexpected Error Calling Start Saga Twice %v", err)
	}

	msgs, err := slog.GetMessages(sagaId)
	if err != nil {
		t.Errorf("Unexpected Error Getting Messages %v", err)
	}

	if len(msgs) != 2 {
		t.Errorf("Expected 1 message for saga, not %v, Messages: %+v", len(msgs), msgs)
	}

	if !isSagaInActiveList(sagaId, slog) {
		t.Errorf("Expected Saga to be in Active List")
	}
}

func TestFullSaga(t *testing.T) {
	defer testCleanup(t)

	dirName := getDirName()
	sagaId := "fullsaga"

	slog, _ := MakeFileSagaLog(dirName)
	jobData := []byte{0, 1, 2, 3, 4, 5}
	slog.StartSaga(sagaId, jobData)

	loggedMsgs := []saga.SagaMessage{
		saga.MakeStartSagaMessage(sagaId, jobData),
		saga.MakeStartTaskMessage(sagaId, "task1", []byte("run task 1")),
		saga.MakeEndTaskMessage(sagaId, "task1", []byte("success")),
		saga.MakeAbortSagaMessage(sagaId),
		saga.MakeStartCompTaskMessage(sagaId, "task1", []byte("rollingback task1")),
		saga.MakeEndCompTaskMessage(sagaId, "task1", []byte("finished rollingback task1")),
		saga.MakeEndSagaMessage(sagaId),
	}

	for i, msg := range loggedMsgs {
		// skip start saga message we already did it
		if i == 0 {
			continue
		}

		err := slog.LogMessage(msg)
		if err != nil {
			t.Fatalf("Unexpected Error Logging Msg: %+v, Error: %v", msg, err)
		}

	}

	rtnMsgs, err := slog.GetMessages(sagaId)
	if err != nil {
		t.Fatalf("Unexpected Error returned from GetMessages. %v", err)
	}

	if len(rtnMsgs) != len(loggedMsgs) {
		t.Fatalf("Expected GetMessages to return %v messages.  Actual %v.  Msgs %+v",
			len(loggedMsgs), len(rtnMsgs), rtnMsgs)
	}

	// we expect the messages to be returned in the same order they are logged
	for j, rtnMsg := range rtnMsgs {
		if !reflect.DeepEqual(loggedMsgs[j], rtnMsg) {
			t.Errorf("Expected Logged Message and Returned Message to be Equal.  Expected %v, Actual %v",
				loggedMsgs[j], rtnMsg)
		}
	}
}

func TestGetMessages_SagaDoesNotExist(t *testing.T) {
	defer testCleanup(t)
	dirName := getDirName()
	slog, _ := MakeFileSagaLog(dirName)
	msgs, err := slog.GetMessages("does_not_exist")

	if err != nil {
		t.Errorf("Unexpected Error Getting Messages %v", err)
	}

	if msgs != nil {
		t.Errorf("Expeceted no messages to be returned %+v", msgs)
	}
}
