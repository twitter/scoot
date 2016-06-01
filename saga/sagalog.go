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

	/*
	 * Called at SagaCoordinator Startup.
	 * returns a list of all in progress Sagas (Start Saga log but
	 * no corresponding End Saga).  May also include completed Sagas
	 */
	Startup() ([]string, error)
}
