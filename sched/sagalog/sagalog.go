package sagalog

import "github.com/scootdev/scoot/messages"

/*
 *  SagaLog Interface, Implemented
 */
type SagaLog interface {

	/*
	 * Log a Start Saga Message to the log.  Returns
	 * an error if it fails.
	 */
	StartSaga(sagaId string, job messages.Job) error

	/*
	 * Update the State of the Saga by Logging a message.
	 * Returns an error if it fails.
	 */
	LogMessage(message SagaMessage) error

	/*
	 * Get the State of the specified saga.
	 * Retuns a SagaState struct.  Modifying this struct
	 * does not update the Saga.  Returns nil if Saga
	 * has not been started, or does not exist.
	 * Returns an error if it fails
	 */
	GetSagaState(sagaId string) (SagaState, error)
}
