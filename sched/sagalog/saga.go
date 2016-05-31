package sagalog

import "github.com/scootdev/scoot/messages"

/*
 * Saga Object which provides all Saga Functionality
 * Implementations of SagaLog should provide a factory method
 * which returns a saga based on its implementation.
 */
type saga struct {
	log SagaLog
}

func (s saga) GetSagaState(sagaId string) (SagaState, error) {
	return s.log.GetSagaState(sagaId)
}

func (s saga) StartSaga(sagaId string, job messages.Job) error {
	return s.log.StartSaga(sagaId, job)
}

/*
 * Log an End Saga Message to the log.  Returns
 * an error if it fails
 */
func (s saga) EndSaga(sagaId string) error {
	entry := SagaMessage{
		sagaId:  sagaId,
		msgType: EndSaga,
	}
	return s.log.LogMessage(entry)
}

/*
 * Log an AbortSaga message.  This indicates that the
 * Saga has failed and all execution should be stopped
 * and compensating transactions should be applied.
 */
func (s saga) AbortSaga(sagaId string) error {
	entry := SagaMessage{
		sagaId:  sagaId,
		msgType: AbortSaga,
	}
	return s.log.LogMessage(entry)
}

/*
 * Log a StartTask Message to the log.  Returns
 * an error if it fails
 */
func (s saga) StartTask(sagaId string, taskId string) error {
	entry := SagaMessage{
		sagaId:  sagaId,
		msgType: StartTask,
		taskId:  taskId,
	}
	return s.log.LogMessage(entry)
}

/*
 * Log an EndTask Message to the log.  Indicates that this task
 * has been successfully completed. Returns an error if it fails.
 */
func (s saga) EndTask(sagaId string, taskId string) error {
	entry := SagaMessage{
		sagaId:  sagaId,
		msgType: EndTask,
		taskId:  taskId,
	}
	return s.log.LogMessage(entry)
}

/*
 * Log a Start a Compensating Task if Saga is aborted, and rollback
 * Is necessary (not using forward recovery).
 */
func (s saga) StartCompensatingTask(sagaId string, taskId string) error {
	entry := SagaMessage{
		sagaId:  sagaId,
		msgType: StartCompTask,
		taskId:  taskId,
	}
	return s.log.LogMessage(entry)
}

/*
 * Log an End Compensating Task message when Compensating task
 * has been successfully completed. Returns an error if it fails.
 */
func (s saga) EndCompensatingTask(sagaId string, taskId string) error {
	entry := SagaMessage{
		sagaId:  sagaId,
		msgType: EndCompTask,
		taskId:  taskId,
	}
	return s.log.LogMessage(entry)
}
