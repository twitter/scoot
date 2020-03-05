package saga

import "fmt"

type SagaRecoveryType int

//
// Saga Recovery Types define how to interpret SagaState in RecoveryMode.
//
// ForwardRecovery: all tasks in the saga must be executed at least once.
//                 tasks MUST BE idempotent
//
// RollbackRecovery: if Saga is Aborted or in unsafe state, compensating
//                  tasks for all started tasks need to be executed.
//                   compensating tasks MUST BE idempotent.
//
const (
	RollbackRecovery SagaRecoveryType = iota
	ForwardRecovery
)

//
// Recovers SagaState from SagaLog messages
//
func recoverState(sagaId string, saga SagaCoordinator) (*SagaState, error) {
	// Get Logged Messages For this Saga from the Log.
	msgs, err := saga.log.GetMessages(sagaId)
	if err != nil {
		return nil, err
	}

	if msgs == nil || len(msgs) == 0 {
		return nil, nil
	}

	// Reconstruct Saga State from Logged Messages
	startMsg := msgs[0]
	if startMsg.MsgType != StartSaga {
		return nil, fmt.Errorf("InvalidMessages: first message must be StartSaga")
	}

	state, err := makeSagaState(sagaId, startMsg.Data)
	if err != nil {
		return nil, err
	}

	for _, msg := range msgs {
		// skip applying StartSaga message we already did this
		// duplicate messages are just ignored since msgs are idempotent
		if msg.MsgType == StartSaga {
			continue
		}

		err = updateSagaState(state, msg)
		if err != nil {
			return nil, err
		}
	}

	return state, nil
}

//
// Returns true if saga is in a safe state, i.e. execution can pick up where
// it left off.  This is only used in RollbackRecovery
//
// A Saga is in a Safe State if all StartedTasks also have EndTask Messages
// A Saga is also in a Safe State if the Saga has been aborted and compensating
// actions have started to be applied.
//
func isSagaInSafeState(state *SagaState) bool {
	if state.IsSagaAborted() {
		return true
	}

	for taskId := range state.taskState {
		if state.IsTaskStarted(taskId) && !state.IsTaskCompleted(taskId) {
			return false
		}
	}

	return true
}
