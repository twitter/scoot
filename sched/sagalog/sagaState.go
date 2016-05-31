package sagalog

import "github.com/scootdev/scoot/messages"

/*
 * Data Structure representation of the current state of the Saga.
 */
type SagaState struct {
	SagaId            string
	Job               messages.Job
	TaskStarted       map[string]bool
	TaskCompleted     map[string]bool
	CompTaskStarted   map[string]bool
	CompTaskCompleted map[string]bool
	SagaAborted       bool
	SagaCompleted     bool
}

/*
 * Initialize a SagaState for the specified saga, and default data.
 */
func SagaStateFactory(sagaId string, job messages.Job) SagaState {
	return SagaState{
		SagaId:            sagaId,
		Job:               job,
		TaskStarted:       make(map[string]bool),
		TaskCompleted:     make(map[string]bool),
		CompTaskStarted:   make(map[string]bool),
		CompTaskCompleted: make(map[string]bool),
		SagaAborted:       false,
		SagaCompleted:     false,
	}
}
