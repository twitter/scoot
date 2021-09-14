package sagalogs

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"reflect"
	"sync"
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

/*
	Set different saga update channel size using DefaultSagaUpdateChSize
	Benchmarked on cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz

	For sagasCount = 1, updatesPerSaga = 1000 and following DefaultSagaUpdateChSize values
	* DefaultSagaUpdateChSize=1:     1 iteration(s)	20359031637 ns/op  44309080 B/op  60419 allocs/op
	* DefaultSagaUpdateChSize=10:	 1 iteration(s)  2209322245 ns/op	6610864 B/op  29788 allocs/op
	* DefaultSagaUpdateChSize=100:	 3 iteration(s)	  368981765 ns/op 	2278261 B/op  23838 allocs/op
	* DefaultSagaUpdateChSize=1000:	 7 iteration(s)	  174756943 ns/op	1861685 B/op  23132 allocs/op

	For sagasCount = 10, updatesPerSaga = 100 and following DefaultSagaUpdateChSize values
	* DefaultSagaUpdateChSize=1:     1 iteration(s)	17434411997 ns/op  6995688 B/op  45573 allocs/op
	* DefaultSagaUpdateChSize=10:	 1 iteration(s)  2553031047 ns/op  2832392 B/op  28176 allocs/op
	* DefaultSagaUpdateChSize=100:	 2 iteration(s)	  705908630 ns/op  1966188 B/op  23760 allocs/op
*/
func BenchmarkProcessUpdatesInFileSagaLog(b *testing.B) {
	dirName := getDirName()
	slog, _ := MakeFileSagaLog(dirName)
	sc := saga.MakeSagaCoordinator(slog)
	sagasCount := 10
	updatesPerSaga := 100

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(sagasCount * updatesPerSaga)

		// start all sagas
		sagas := []*saga.Saga{}
		for k := 0; k < sagasCount; k++ {
			saga, _ := sc.MakeSaga(fmt.Sprintf("testSaga%d", k), nil)
			sagas = append(sagas, saga)
		}

		// Start multiple tasks on multiple sagas
		for _, saga := range sagas {
			for j := 0; j < updatesPerSaga; j++ {
				go func(taskID int) {
					defer wg.Done()
					_ = saga.StartTask(fmt.Sprintf("task%d", taskID), nil)
				}(j)
			}
		}

		//Wait for tasks to finish before calling EndSaga
		wg.Wait()
		for _, saga := range sagas {
			_ = saga.EndSaga()
		}
	}
}
