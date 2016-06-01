package saga

/*
 *  SagaLog Interface, Implemented
 */
type SagaLog interface {

	/*
	 * Log a Start Saga Message message to the log.
	 * Returns an error if it fails.
	 */
	StartSaga(sagaId string, job []byte) error

	/*
	 * Update the State of the Saga by Logging a message.
	 * Returns an error if it fails.
	 */
	LogMessage(message sagaMessage) error

	/*
	 * Returns all of the messages logged so far for the
	 * specified saga.
	 */
	GetMessages(sagaId string) ([]sagaMessage, error)
}
