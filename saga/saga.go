package saga

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
 * Returns the resulting SagaState or an error if it fails.
 */
func (s Saga) StartSaga(sagaId string, job []byte) (*SagaState, error) {

	//Create new SagaState
	state, err := makeSagaState(sagaId, job)
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
 * Log an End Saga Message to the log, returns updated SagaState
 * Returns the resulting SagaState or an error if it fails
 */
func (s Saga) EndSaga(state *SagaState) (*SagaState, error) {
	return s.logMessage(state, MakeEndSagaMessage(state.sagaId))
}

/*
 * Log an AbortSaga message.  This indicates that the
 * Saga has failed and all execution should be stopped
 * and compensating transactions should be applied.
 *
 * Returns the resulting SagaState or an error if it fails
 */
func (s Saga) AbortSaga(state *SagaState) (*SagaState, error) {

	return s.logMessage(state, MakeAbortSagaMessage(state.sagaId))
}

/*
 * Log a StartTask Message to the log.  Returns
 * an error if it fails.
 *
 * StartTask is idempotent with respect to sagaId & taskId.  If
 * the data passed changes the last written StartTask message will win
 *
 * Returns the resulting SagaState or an error if it fails
 */
func (s Saga) StartTask(state *SagaState, taskId string, data []byte) (*SagaState, error) {
	return s.logMessage(state, MakeStartTaskMessage(state.sagaId, taskId, data))
}

/*
 * Log an EndTask Message to the log.  Indicates that this task
 * has been successfully completed. Returns an error if it fails.
 *
 * EndTask is idempotent with respect to sagaId & taskId.  If
 * the data passed changes the last written EndTask message will win
 *
 * Returns the resulting SagaState or an error if it fails
 */
func (s Saga) EndTask(state *SagaState, taskId string, results []byte) (*SagaState, error) {
	return s.logMessage(state, MakeEndTaskMessage(state.sagaId, taskId, results))
}

/*
 * Log a Start Compensating Task Message to the log. Should only be logged after a Saga
 * has been avoided and in Rollback Recovery Mode. Should not be used in ForwardRecovery Mode
 * returns an error if it fails
 *
 * StartCompTask is idempotent with respect to sagaId & taskId.  If
 * the data passed changes the last written StartCompTask message will win
 *
 * Returns the resulting SagaState or an error if it fails
 */
func (s Saga) StartCompensatingTask(state *SagaState, taskId string, data []byte) (*SagaState, error) {
	return s.logMessage(state, MakeStartCompTaskMessage(state.sagaId, taskId, data))
}

/*
 * Log an End Compensating Task Message to the log when a Compensating Task
 * has been successfully completed. Returns an error if it fails.
 *
 * EndCompTask is idempotent with respect to sagaId & taskId.  If
 * the data passed changes the last written EndCompTask message will win
 *
 * Returns the resulting SagaState or an error if it fails
 */
func (s Saga) EndCompensatingTask(state *SagaState, taskId string, results []byte) (*SagaState, error) {
	return s.logMessage(state, MakeEndCompTaskMessage(state.sagaId, taskId, results))
}

/*
 * Should be called at Saga Creation time.
 * Returns a Slice of In Progress SagaIds
 */
func (s Saga) Startup() ([]string, error) {

	ids, err := s.log.GetActiveSagas()
	if err != nil {
		return nil, err
	}

	return ids, nil
}

/*
 * Recovers SagaState by reading all logged messages from the log.
 * Utilizes the specified recoveryType to determine if Saga needs to be
 * Aborted or can proceed safely.
 *
 * Returns the current SagaState
 */
func (s Saga) RecoverSagaState(sagaId string, recoveryType SagaRecoveryType) (*SagaState, error) {
	return recoverState(sagaId, s, recoveryType)
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
