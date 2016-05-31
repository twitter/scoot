package saga

type SagaMessageType int

const (
	StartSaga SagaMessageType = iota
	EndSaga
	AbortSaga
	StartTask
	EndTask
	StartCompTask
	EndCompTask
)

func (s SagaMessageType) String() string {
	switch s {
	case StartSaga:
		return "Start Saga"
	case EndSaga:
		return "End Saga"
	case AbortSaga:
		return "Abort Saga"
	case StartTask:
		return "Start Task"
	case EndTask:
		return "End Task"
	case StartCompTask:
		return "Start Comp Task"
	case EndCompTask:
		return "End Comp Task"
	default:
		return "unknown"
	}
}

/*
 * Data Structure representation of a entry in the SagaLog.
 * Different SagaMessageTypes utilize different fields.
 * Factory Methods are supplied for creation of Saga Messages
 * and should be used instead of directly creatinga  sagaMessage struct
 */
type sagaMessage struct {
	sagaId  string
	msgType SagaMessageType
	data    []byte
	taskId  string
}

/*
 * StartSaga SagaMessageType
 * 	- sagaId - id of the Saga
 *  - data - data needed to execute the saga
 */
func StartSagaMessageFactory(sagaId string, job []byte) sagaMessage {
	return sagaMessage{
		sagaId:  sagaId,
		msgType: StartSaga,
		data:    job,
	}
}

/*
 * EndSaga SagaMessageType
 *  - sagaId - id of the Saga
 */
func EndSagaMessageFactory(sagaId string) sagaMessage {
	return sagaMessage{
		sagaId:  sagaId,
		msgType: EndSaga,
	}
}

/*
 * AbortSaga SagaMessageType
 *  - sagaId - id of the Saga
 */
func AbortSagaMessageFactory(sagaId string) sagaMessage {
	return sagaMessage{
		sagaId:  sagaId,
		msgType: AbortSaga,
	}
}

/*
 * StartTask SagaMessageType
 *  - sagaId - id of the Saga
 *  - taskId - id of the started Task
 */
func StartTaskMessageFactory(sagaId string, taskId string) sagaMessage {
	return sagaMessage{
		sagaId:  sagaId,
		msgType: StartTask,
		taskId:  taskId,
	}
}

/*
 * EndTask SagaMessageType
 *  - sagaId - id of the Saga
 *  - taskId - id of the completed Task
 *  - data - any results from task completion
 */
func EndTaskMessageFactory(sagaId string, taskId string, results []byte) sagaMessage {
	return sagaMessage{
		sagaId:  sagaId,
		msgType: EndTask,
		taskId:  taskId,
		data:    results,
	}
}

/*
 * StartCompTask SagaMessageType
 *  - sagaId - id of the Saga
 *  - taskId - id of the started compensating task.  Should
 *             be the same as the original taskId
 */
func StartCompTaskMessageFactory(sagaId string, taskId string) sagaMessage {
	return sagaMessage{
		sagaId:  sagaId,
		msgType: StartCompTask,
		taskId:  taskId,
	}
}

/*
 * EndCompTask SagaMessageType
 *  - sagaId - id of the Saga
 *  - taskId - id of the completed compensating task.  Should
 *             be the same as the original taskId
 *  - data - any results from compensating task completion
 */
func EndCompTaskMessageFactory(sagaId string, taskId string, results []byte) sagaMessage {
	return sagaMessage{
		sagaId:  sagaId,
		msgType: EndCompTask,
		taskId:  taskId,
		data:    results,
	}
}
