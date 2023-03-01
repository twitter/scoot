package sagalogs

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/twitter/scoot/saga"
)

func TestMemorySagaParams(t *testing.T) {
	// OK
	MakeInMemorySagaLogNoGC()
	MakeInMemorySagaLog(0, 0)
	MakeInMemorySagaLog(0, 1)
	MakeInMemorySagaLog(1*time.Hour, 1*time.Second)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Expected panic from did not happen")
		}
	}()

	// Non-zero expiration with zero interval should panic
	MakeInMemorySagaLog(1, 0)
}

func TestMemorySagaStartLogGetMessages(t *testing.T) {
	slog := MakeInMemorySagaLogNoGC()

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

func TestMemorySagaGC(t *testing.T) {
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

/*
Set different saga update channel size using DefaultSagaUpdateChSize
Benchmarked on cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz

For sagasCount = 10, updatesPerSaga = 10000 and following DefaultSagaUpdateChSize values
* DefaultSagaUpdateChSize=1:     1 iteration(s)	124623537403 ns/op
* DefaultSagaUpdateChSize=10:	 1 iteration(s)  12299526501 ns/op
* DefaultSagaUpdateChSize=100:	 1 iteration(s)	  1864785416 ns/op
* DefaultSagaUpdateChSize=1000:	 3 iteration(s)	   451137271 ns/op
* DefaultSagaUpdateChSize=10000: 7 iteration(s)	   176294414 ns/op

For sagasCount = 1000, updatesPerSaga = 100 and following DefaultSagaUpdateChSize values
* DefaultSagaUpdateChSize=1:     1 iteration(s)	1770828443 ns/op
* DefaultSagaUpdateChSize=10:	 3 iteration(s)  392772272 ns/op
* DefaultSagaUpdateChSize=100:	 6 iteration(s)	 180072440 ns/op
*/
func BenchmarkProcessUpdatesInMemorySagaLog(b *testing.B) {
	slog := MakeInMemorySagaLog(1*time.Hour, 1*time.Hour)
	sc := saga.MakeSagaCoordinator(slog, nil)
	sagasCount := 10
	updatesPerSaga := 10000

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(sagasCount * updatesPerSaga)

		// start all sagas
		sagas := []*saga.Saga{}
		for k := 0; k < sagasCount; k++ {
			s, _ := sc.MakeSaga(fmt.Sprintf("testSaga%d", k), nil)
			sagas = append(sagas, s)
		}

		// Start multiple tasks on multiple sagas
		for _, s := range sagas {
			for j := 0; j < updatesPerSaga; j++ {
				go func(taskID int) {
					defer wg.Done()
					_ = s.StartTask(fmt.Sprintf("task%d", taskID), nil)
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
