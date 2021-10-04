// Package sagalogs provides implementations of SagaLog.
package sagalogs

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/saga"

	log "github.com/sirupsen/logrus"
)

// In Memory Implementation of a SagaLog, DOES NOT durably persist messages.
type inMemorySagaLog struct {
	sagas        map[string]*logData
	mutex        sync.RWMutex
	gcExpiration time.Duration
	gcTicker     *time.Ticker
}

// wrapper for SagaMessages tracking timestamps for GC
type logData struct {
	messages []saga.SagaMessage
	created  time.Time
}

// Returns an Instance of a Saga based on an inMemorySagaLog.
// This is an in-memory impl, and is not durable.
// Implements GC of whole sagas based on time expiration regardless of Saga state.
// GC should be set at a duration that realistically will not purge active Sagas.
// gcExpiration: duration after which a Saga was created, it will be deleted.
// 	A zero duration is interpretted as "never gc" (the Log will eventually consume all memory).
// gcInterval: duration interval at which GC runs.
func MakeInMemorySagaCoordinator(gcExpiration time.Duration, gcInterval time.Duration, stat stats.StatsReceiver) saga.SagaCoordinator {
	return saga.MakeSagaCoordinator(MakeInMemorySagaLog(gcExpiration, gcInterval), stat)
}

// Make an InMemorySagaLog with specified GC expiration and interval duration.
func MakeInMemorySagaLog(gcExpiration time.Duration, gcInterval time.Duration) saga.SagaLog {
	slog := &inMemorySagaLog{
		sagas:        make(map[string]*logData),
		mutex:        sync.RWMutex{},
		gcExpiration: gcExpiration,
	}
	if gcExpiration != 0 {
		slog.gcTicker = time.NewTicker(gcInterval)
		go func() {
			for range slog.gcTicker.C {
				if err := slog.gcSagas(); err != nil {
					log.Errorf("Error running gcSagas: %s", err)
				}
			}
		}()
	}
	return slog
}

// Shorthand creator function to create a non-GCing SagaLog with Coordinator
func MakeInMemorySagaCoordinatorNoGC(stat stats.StatsReceiver) saga.SagaCoordinator {
	return saga.MakeSagaCoordinator(MakeInMemorySagaLogNoGC(), stat)
}

// Shorthand creator function to create a non-GCing SagaLog
func MakeInMemorySagaLogNoGC() saga.SagaLog {
	return MakeInMemorySagaLog(0, 0)
}

// Creates a Saga in the log and adds a StartSagaMessage to it
func (slog *inMemorySagaLog) StartSaga(sagaId string, job []byte) error {
	slog.mutex.Lock()
	defer slog.mutex.Unlock()

	startMsg := saga.MakeStartSagaMessage(sagaId, job)
	slog.sagas[sagaId] = &logData{messages: []saga.SagaMessage{startMsg}, created: time.Now()}
	return nil
}

// Log a SagaMessage to an existing Saga in the log
func (slog *inMemorySagaLog) LogMessage(msg saga.SagaMessage) error {
	return slog.logMessages([]saga.SagaMessage{msg})
}

// Log a batch of messages in one transaction. Assumes messages are for the same saga.
func (slog *inMemorySagaLog) LogBatchMessages(msgs []saga.SagaMessage) error {
	return slog.logMessages(msgs)
}

func (slog *inMemorySagaLog) logMessages(msgs []saga.SagaMessage) error {
	if len(msgs) == 0 {
		return errors.New("Empty messages slice passed to logMessages")
	}

	slog.mutex.Lock()
	defer slog.mutex.Unlock()

	sagaId := msgs[0].SagaId
	ld, ok := slog.sagas[sagaId]
	if !ok {
		return errors.New(fmt.Sprintf("Saga: %s does not exist in the Log", sagaId))
	}

	ld.messages = append(ld.messages, msgs...)
	return nil
}

// Gets all SagaMessages from an existing Saga in the log
func (slog *inMemorySagaLog) GetMessages(sagaId string) ([]saga.SagaMessage, error) {
	slog.mutex.RLock()
	defer slog.mutex.RUnlock()

	ld, ok := slog.sagas[sagaId]
	if ok {
		return ld.messages, nil
	} else {
		return nil, nil
	}
}

// Returns all non-GCd SagaIds existing since this SagaLog was created.
// Includes Sagas of any state (completed, active, etc).
func (slog *inMemorySagaLog) GetActiveSagas() ([]string, error) {
	slog.mutex.RLock()
	defer slog.mutex.RUnlock()

	keys := make([]string, len(slog.sagas))
	i := 0

	for key := range slog.sagas {
		keys[i] = key
		i++
	}

	return keys, nil
}

// Check for expired Sagas and then delete them.
// Sagas need not be completed to be GCd.
func (slog *inMemorySagaLog) gcSagas() error {
	expired := slog.getExpiredSagaIds()
	if len(expired) == 0 {
		return nil
	}

	slog.mutex.Lock()
	defer slog.mutex.Unlock()

	for _, id := range expired {
		delete(slog.sagas, id)
	}

	return nil
}

func (slog *inMemorySagaLog) getExpiredSagaIds() []string {
	slog.mutex.RLock()
	defer slog.mutex.RUnlock()

	expired := []string{}
	for id, ld := range slog.sagas {
		if time.Since(ld.created) >= slog.gcExpiration {
			expired = append(expired, id)
		}
	}

	return expired
}

// Private utility function for testing only
func (slog *inMemorySagaLog) setSagaCreatedTime(sagaId string, created time.Time) {
	slog.mutex.Lock()
	defer slog.mutex.Unlock()

	ld, ok := slog.sagas[sagaId]
	if !ok {
		return
	}
	ld.created = created
}
