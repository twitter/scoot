package saga

import (
	"errors"
	"fmt"
	"sync"
)

/*
 * In Memory Implementation of a Saga Log, DOES NOT durably persist messages.
 * This is for local testing purposes.
 */
type inMemorySagaLog struct {
	sagas map[string][]sagaMessage
	mutex *sync.RWMutex
}

/*
 * Returns an Instance of a Saga based on an InMemorySagaLog
 */
func MakeInMemorySaga() Saga {
	inMemLog := inMemorySagaLog{
		sagas: make(map[string][]sagaMessage),
		mutex: &sync.RWMutex{},
	}
	return MakeSaga(&inMemLog)
}

func (log *inMemorySagaLog) LogMessage(msg sagaMessage) error {
	fmt.Println(fmt.Sprintf("Saga %s: %s %s", msg.sagaId, msg.msgType.String(), msg.taskId))

	sagaId := msg.sagaId
	var err error

	log.mutex.Lock()
	msgs, ok := log.sagas[sagaId]
	if !ok {
		return errors.New(fmt.Sprintf("Saga: %s is not Started yet.", msg.sagaId))
	}

	log.sagas[sagaId] = append(msgs, msg)
	log.mutex.Unlock()
	return err
}

func (log *inMemorySagaLog) StartSaga(sagaId string, job []byte) error {

	log.mutex.Lock()

	fmt.Println(fmt.Sprintf("Start Saga %s", sagaId))

	startMsg := MakeStartSagaMessage(sagaId, job)
	log.sagas[sagaId] = []sagaMessage{startMsg}

	log.mutex.Unlock()

	return nil
}

func (log *inMemorySagaLog) GetMessages(sagaId string) ([]sagaMessage, error) {

	log.mutex.RLock()
	msgs, ok := log.sagas[sagaId]
	log.mutex.RUnlock()

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
	keys := make([]string, 0, len(log.sagas))

	for key, _ := range log.sagas {
		keys = append(keys, key)
	}

	log.mutex.RUnlock()

	return keys, nil
}
