package saga

/*
 * Saga Object which provides all Saga Functionality
 * Implementations of SagaLog should provide a factory method
 * which returns a saga based on its implementation.
 */
type Saga struct {
	log SagaLog
}

func (s Saga) GetSagaState(sagaId string) (*SagaState, error) {
	return s.log.GetSagaState(sagaId)
}

func (s Saga) StartSaga(sagaId string, job []byte) error {
	return s.log.StartSaga(sagaId, job)
}

/*
 * Log an End Saga Message to the log.  Returns
 * an error if it fails
 */
func (s Saga) EndSaga(sagaId string) error {
	return s.log.LogMessage(EndSagaMessageFactory(sagaId))
}

/*
 * Log an AbortSaga message.  This indicates that the
 * Saga has failed and all execution should be stopped
 * and compensating transactions should be applied.
 */
func (s Saga) AbortSaga(sagaId string) error {
	return s.log.LogMessage(AbortSagaMessageFactory(sagaId))
}

/*
 * Log a StartTask Message to the log.  Returns
 * an error if it fails
 */
func (s Saga) StartTask(sagaId string, taskId string) error {
	return s.log.LogMessage(StartTaskMessageFactory(sagaId, taskId))
}

/*
 * Log an EndTask Message to the log.  Indicates that this task
 * has been successfully completed. Returns an error if it fails.
 */
func (s Saga) EndTask(sagaId string, taskId string, results []byte) error {
	return s.log.LogMessage(EndTaskMessageFactory(sagaId, taskId, results))
}

/*
 * Log a Start a Compensating Task if Saga is aborted, and rollback
 * Is necessary (not using forward recovery).
 */
func (s Saga) StartCompensatingTask(sagaId string, taskId string) error {
	return s.log.LogMessage(StartCompTaskMessageFactory(sagaId, taskId))
}

/*
 * Log an End Compensating Task message when Compensating task
 * has been successfully completed. Returns an error if it fails.
 */
func (s Saga) EndCompensatingTask(sagaId string, taskId string, results []byte) error {
	return s.log.LogMessage(EndCompTaskMessageFactory(sagaId, taskId, results))
}
