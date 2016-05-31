package sagalog

import "github.com/scootdev/scoot/messages"

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

/*
 * Data Structure representation of a entry in the
 * Saga Log
 */
type SagaMessage struct {
	sagaId  string
	msgType SagaMessageType
	job     messages.Job
	taskId  string
}
