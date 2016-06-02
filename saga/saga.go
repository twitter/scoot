package saga

type SagaRecoveryType int

/*
 * Saga Recovery Types define how to interpret SagaState in RecoveryMode.
 *
 * ForwardRecovery: all tasks in the saga must be executed at least once.
 *                  tasks MUST BE idempotent
 *
 * RollbackRecovery: if Saga is Aborted or in unsafe state, compensating
 *                   tasks for all started tasks need to be executed.
 *                   compensating tasks MUST BE idempotent.
 */
const (
	BackwardRecovery SagaRecoveryType = iota
	ForwardRecovery
)

/*
 * Saga Object which provides all Saga Functionality
 * Implementations of SagaLog should provide a factory method
 * which returns a saga based on its implementation.
 */
type Saga struct {
	log SagaLog
}

/*
 * Make a Saga which uses the specied SagaLog interface for durable storage
 */
func MakeSaga(log SagaLog) Saga {
	return Saga{
		log: log,
	}
}

/*
 * Start Saga. Logs Message message to the log.
 * Returns an error if it fails.
 */
func (s Saga) StartSaga(sagaId string, job []byte) (*SagaState, error) {

	//Create new SagaState
	state, err := sagaStateFactory(sagaId, job)
	if err != nil {
		return nil, err
	}

	//Durably Store that we Created a new Saga
	err = s.log.StartSaga(sagaId, job)
	if err != nil {
		return nil, err
	}

	return state, nil
}

/*
 * logs the specified message durably to the SagaLog & updates internal state if its a valid state transition
 */
func (s Saga) logMessage(state *SagaState, msg sagaMessage) (*SagaState, error) {

	//verify that the applied message results in a valid state
	newState, err := updateSagaState(state, msg)
	if err != nil {
		return nil, err
	}

	//try durably storing the message
	err = s.log.LogMessage(msg)
	if err != nil {
		return nil, err
	}

	return newState, nil
}

/*
 * Log an End Saga Message to the log.  Returns
 * an error if it fails
 */
func (s Saga) EndSaga(state *SagaState) (*SagaState, error) {
	return s.logMessage(state, MakeEndSagaMessage(state.sagaId))
}

/*
 * Log an AbortSaga message.  This indicates that the
 * Saga has failed and all execution should be stopped
 * and compensating transactions should be applied.
 */
func (s Saga) AbortSaga(state *SagaState) (*SagaState, error) {

	return s.logMessage(state, MakeAbortSagaMessage(state.sagaId))
}

/*
 * Log a StartTask Message to the log.  Returns
 * an error if it fails
 */
func (s Saga) StartTask(state *SagaState, taskId string) (*SagaState, error) {
	return s.logMessage(state, MakeStartTaskMessage(state.sagaId, taskId))
}

/*
 * Log an EndTask Message to the log.  Indicates that this task
 * has been successfully completed. Returns an error if it fails.
 */
func (s Saga) EndTask(state *SagaState, taskId string, results []byte) (*SagaState, error) {
	return s.logMessage(state, MakeEndTaskMessage(state.sagaId, taskId, results))
}

/*
 * Log a Start a Compensating Task if Saga is aborted, and rollback
 * Is necessary (not using forward recovery).
 */
func (s Saga) StartCompensatingTask(state *SagaState, taskId string) (*SagaState, error) {
	return s.logMessage(state, MakeStartCompTaskMessage(state.sagaId, taskId))
}

/*
 * Log an End Compensating Task message when Compensating task
 * has been successfully completed. Returns an error if it fails.
 */
func (s Saga) EndCompensatingTask(state *SagaState, taskId string, results []byte) (*SagaState, error) {
	return s.logMessage(state, MakeEndCompTaskMessage(state.sagaId, taskId, results))
}
