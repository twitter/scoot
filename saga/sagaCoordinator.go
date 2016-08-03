package saga

//
// Saga Object which provides all Saga Functionality
// Implementations of SagaLog should provide a factory method
// which returns a saga based on its implementation.
//
type SagaCoordinator struct {
	log SagaLog
}

//
// Make a Saga which uses the specied SagaLog interface for durable storage
//
func MakeSagaCoordinator(log SagaLog) SagaCoordinator {
	return SagaCoordinator{
		log: log,
	}
}

func (s SagaCoordinator) MakeEmptySaga(sagaId string) *Saga {
	return newEmptySaga(sagaId, s.log)
}

// Make a Saga add it to the SagaCoordinator, if a Saga Already exists
// with the same id, it will overwrite the already existing one.
func (s SagaCoordinator) MakeSaga(sagaId string, job []byte) (*Saga, error) {
	return newSaga(sagaId, job, s.log)
}

// Read the Current SagaState from the Log, intended for status queries does not check for recovery.
// RecoverSagaState should be used for recovering state in a failure scenario
func (s SagaCoordinator) GetSagaState(sagaId string) (*SagaState, error) {
	return recoverState(sagaId, s)
}

//
// Should be called at Saga Creation time.
// Returns a Slice of In Progress SagaIds
//
func (s SagaCoordinator) Startup() ([]string, error) {

	ids, err := s.log.GetActiveSagas()
	if err != nil {
		return nil, err
	}

	return ids, nil
}

//
// Recovers SagaState by reading all logged messages from the log.
// Utilizes the specified recoveryType to determine if Saga needs to be
// Aborted or can proceed safely.
//
// Returns the current SagaState
//
func (sc SagaCoordinator) RecoverSagaState(sagaId string, recoveryType SagaRecoveryType) (*Saga, error) {
	state, err := recoverState(sagaId, sc)

	if err != nil {
		return nil, err
	}

	// now that we've recovered the saga initialize its update path
	saga := rehydrateSaga(sagaId, state, sc.log)

	// Check if we can safely proceed forward based on recovery method
	// RollbackRecovery must check if in a SafeState,
	// ForwardRecovery can always make progress
	switch recoveryType {

	case RollbackRecovery:

		// if Saga is not in a safe state we must abort the saga
		// And compensating tasks should start
		if !isSagaInSafeState(state) {
			err = saga.AbortSaga()
			if err != nil {
				return nil, err
			}
		}

	case ForwardRecovery:
		// Nothing to do on Forward Recovery
	}

	return saga, err
}
