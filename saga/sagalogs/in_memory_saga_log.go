// Package sagalogs provides an implementation of Saga Log.
// Currently, this is an in-memory impl, and is not durable.
package sagalogs

import (
	"errors"
	"fmt"
	"github.com/twitter/scoot/saga"
	"sync"
)

/*
 * In Memory Implementation of a Saga Log, DOES NOT durably persist messages.
 * This is for local testing purposes.
 */
type inMemorySagaLog struct {
	sagas map[string][]saga.SagaMessage
	mutex sync.RWMutex
}

/*
 * Returns an Instance of a Saga based on an InMemorySagaLog
 */
func MakeInMemorySagaCoordinator() saga.SagaCoordinator {
	return saga.MakeSagaCoordinator(MakeInMemorySagaLog())
}

func MakeInMemorySagaLog() saga.SagaLog {
	return &inMemorySagaLog{
		sagas: make(map[string][]saga.SagaMessage),
		mutex: sync.RWMutex{},
	}
}

func (log *inMemorySagaLog) LogMessage(msg saga.SagaMessage) error {

	log.mutex.Lock()
	defer log.mutex.Unlock()

	sagaId := msg.SagaId

	msgs, ok := log.sagas[sagaId]
	if !ok {
		return errors.New(fmt.Sprintf("Saga: %s is not Started yet.", msg.SagaId))
	}

	log.sagas[sagaId] = append(msgs, msg)
	return nil
}

func (log *inMemorySagaLog) StartSaga(sagaId string, job []byte) error {

	log.mutex.Lock()
	defer log.mutex.Unlock()

	startMsg := saga.MakeStartSagaMessage(sagaId, job)
	log.sagas[sagaId] = []saga.SagaMessage{startMsg}

	return nil
}

func (log *inMemorySagaLog) GetMessages(sagaId string) ([]saga.SagaMessage, error) {

	log.mutex.RLock()
	defer log.mutex.RUnlock()

	msgs, ok := log.sagas[sagaId]

	if ok {
		return msgs, nil
	} else {
		return nil, nil
	}
}

/*
 * Returns all Sagas Started since this InMemory Saga was created
 */
func (log *inMemorySagaLog) GetActiveSagas() ([]string, error) {
	log.mutex.RLock()
	defer log.mutex.RUnlock()

	keys := make([]string, 0, len(log.sagas))

	for key, _ := range log.sagas {
		keys = append(keys, key)
	}

	return keys, nil
}
