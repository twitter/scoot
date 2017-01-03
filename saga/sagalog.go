package saga

//go:generate mockgen -source=sagalog.go -package=saga -destination=sagalog_mock.go

import (
	"fmt"
)

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
	LogMessage(message SagaMessage) error

	/*
	 * Returns all of the messages logged so far for the
	 * specified saga.
	 */
	GetMessages(sagaId string) ([]SagaMessage, error)

	/*
	 * Returns a list of all in progress sagaIds.
	 * This MUST include all not completed sagaIds.
	 * It may also included completed sagas
	 * Returns an error if it fails.
	 */
	GetActiveSagas() ([]string, error)
}

// CorruptedSagaLogError this is a critical error specifies
// that the data stored in the sagalog for a specified saga
// is corrupted and unrecoverable.
type CorruptedSagaLogError struct {
	msg string
	id  string
}

func (e CorruptedSagaLogError) Error() string {
	return fmt.Sprintf("SagaLog for %v has been corrupted, Error: %v", e.id, e.msg)
}

func NewCorruptedSagaLogError(sagaId string, msg string) error {
	return CorruptedSagaLogError{
		msg: msg,
		id:  sagaId,
	}
}

// InvalidRequestError should be returned by the SagaLog
// when the request is invalid and the same request will
// fail on restart, equivalent to an HTTP 400
type InvalidRequestError struct {
	s string
}

func (e InvalidRequestError) Error() string {
	return e.s
}

func NewInvalidRequestError(msg string) error {
	return InvalidRequestError{
		s: msg,
	}
}

// InternalLogError should be returned by the SagaLog
// when the request failed, but may succeed on retry
// this is equivalent to an HTTP 500
type InternalLogError struct {
	s string
}

func (e InternalLogError) Error() string {
	return e.s
}

func NewInternalLogError(msg string) error {
	return InternalLogError{
		s: msg,
	}
}
