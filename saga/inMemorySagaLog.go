package saga

import "errors"
import "fmt"
import "sync"

/*
 * In Memory Implementation of a Saga Log, DOES NOT durably persist messages.
 * This is for local testing purposes.
 */
type inMemorySagaLog struct {
	sagas map[string]*SagaState
	mutex *sync.RWMutex
}

/*
 * Returns an Instance of a Saga based on an InMemorySagaLog
 */
func InMemorySagaFactory() Saga {
	inMemLog := inMemorySagaLog{
		sagas: make(map[string]*SagaState),
		mutex: &sync.RWMutex{},
	}
	return Saga{
		log: &inMemLog,
	}
}

func (log *inMemorySagaLog) LogMessage(msg sagaMessage) error {
	fmt.Println(fmt.Sprintf("Saga %s: %s %s", msg.sagaId, msg.msgType, msg.taskId))

	sagaId := msg.sagaId
	var err error

	log.mutex.RLock()
	sagaState, ok := log.sagas[sagaId]

	if ok {
		err = sagaState.UpdateSagaState(msg)
	} else {
		err = errors.New(fmt.Sprintf("Cannot Log Saga Message %i, Never Started Saga %s", msg.msgType, sagaId))
	}

	log.mutex.RUnlock()
	return err
}

func (log *inMemorySagaLog) StartSaga(sagaId string, job []byte) error {

	log.mutex.Lock()

	fmt.Println(fmt.Sprintf("Start Saga %s", sagaId))
	sagaState, _ := SagaStateFactory(sagaId, job)
	log.sagas[sagaId] = sagaState

	log.mutex.Unlock()

	return nil
}

func (log *inMemorySagaLog) GetSagaState(sagaId string) (*SagaState, error) {

	log.mutex.RLock()
	sagaState, ok := log.sagas[sagaId]
	log.mutex.RUnlock()

	if ok {
		return sagaState, nil
	} else {
		return sagaState, errors.New(fmt.Sprintf("Saga %s Does Not Exist", sagaId))
	}
}
