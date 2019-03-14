package sagalogs

import (
	"reflect"
	"testing"
	"time"

	"github.com/twitter/scoot/saga"
)

func TestMemoryStartLogGetMessages(t *testing.T) {
	slog := MakeInMemorySagaLog(0, 1)

	sagaId := "s1"
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
			t.Fatalf("Unexpected error logging saga message: %s", err)
		}
	}

	if !isSagaInActiveList(sagaId, slog) {
		t.Fatalf("Expected Saga %s to be in active list", sagaId)
	}

	outMsgs, err := slog.GetMessages(sagaId)
	if err != nil {
		t.Fatalf("Unexpected error getting saga messages: %s", err)
	}
	if outMsgs == nil {
		t.Fatalf("Unexpected nil result from GetMessages")
	}
	if len(outMsgs) != len(loggedMsgs) {
		t.Fatalf("GetMessages length did not match loggedMsgs: %d/%d", len(outMsgs), len(loggedMsgs))
	}

	// we expect the messages to be returned in the same order they are logged
	for j, outMsg := range outMsgs {
		if !reflect.DeepEqual(loggedMsgs[j], outMsg) {
			t.Errorf("Expected Logged Message and Returned Message to be Equal.  Expected %v, Actual %v",
				loggedMsgs[j], outMsg)
		}
	}
}

func TestMemoryGC(t *testing.T) {
	// set GC slow enough that it won't happen during the test
	slog := MakeInMemorySagaLog(1*time.Hour, 1*time.Hour)

	sagaId1 := "s1"
	sagaId2 := "s2"
	jobData := []byte{0, 1, 2, 3, 4, 5}
	slog.StartSaga(sagaId1, jobData)
	slog.StartSaga(sagaId2, jobData)

	// verify sagas started
	if !isSagaInActiveList(sagaId1, slog) {
		t.Fatalf("Expected Saga %s to be in active list", sagaId1)
	}
	if !isSagaInActiveList(sagaId2, slog) {
		t.Fatalf("Expected Saga %s to be in active list", sagaId2)
	}

	// reset s1's created time to be older than our GC expiration
	slog.(*inMemorySagaLog).setSagaCreatedTime(sagaId1, time.Now().Add(-2*time.Hour))

	// invoke the GC
	err := slog.(*inMemorySagaLog).gcSagas()
	if err != nil {
		t.Fatalf("Unexpected error running GC: %s", err)
	}

	// verify only the "old" saga is gone
	if isSagaInActiveList(sagaId1, slog) {
		t.Fatalf("Expected Saga %s NOT to be in active list", sagaId1)
	}
	if !isSagaInActiveList(sagaId2, slog) {
		t.Fatalf("Expected Saga %s to be in active list", sagaId2)
	}

	// test logging to GC'd saga results in an error
	err = slog.LogMessage(saga.MakeEndSagaMessage(sagaId1))
	if err == nil {
		t.Fatalf("Unexpected nil error logging message to GC'd saga")
	}
}
