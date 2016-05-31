package sagalog

import "errors"
import "fmt"
import "github.com/scootdev/scoot/messages"

/*
 * In Memory Implementation of a Saga Log, DOES NOT durably persist messages.
 * This is for local testing purposes.
 */
type inMemorySagaLog struct {
	sagas map[string]SagaState
}

/*
 * Returns an Instance of a Saga based on an InMemorySagaLog
 */
func InMemorySagaFactory() saga {
	inMemLog := inMemorySagaLog{
		sagas: make(map[string]SagaState),
	}
	return saga{
		log: &inMemLog,
	}
}

func (log *inMemorySagaLog) LogMessage(msg SagaMessage) error {
	sagaId := msg.sagaId
	sagaState, ok := log.sagas[sagaId]

	if ok {

		switch msg.msgType {
		case StartSaga:

		case EndSaga:
			sagaState.SagaCompleted = true
		case AbortSaga:
			sagaState.SagaAborted = true
		case StartTask:
			sagaState.TaskStarted[msg.taskId] = true
		case EndTask:
			sagaState.TaskCompleted[msg.taskId] = true
		case StartCompTask:
			sagaState.CompTaskStarted[msg.taskId] = true
		case EndCompTask:
			sagaState.CompTaskCompleted[msg.taskId] = true
		}

		return nil
	} else {
		return errors.New(fmt.Sprintf("Cannot Log Saga Message %i, Never Started Saga %s", msg.msgType, sagaId))
	}
}

func (log *inMemorySagaLog) StartSaga(sagaId string, job messages.Job) error {

	_, ok := log.sagas[sagaId]
	if ok {
		return errors.New("Saga Already Exists")
	}

	sagaState := SagaStateFactory(sagaId, job)
	log.sagas[sagaId] = sagaState
	return nil
}

func (log *inMemorySagaLog) GetSagaState(sagaId string) (SagaState, error) {

	sagaState, ok := log.sagas[sagaId]

	if ok {
		return sagaState, nil
	} else {
		return sagaState, errors.New(fmt.Sprintf("Saga %s Does Not Exist", sagaId))
	}
}
