package saga

import (
	"errors"
	"fmt"
)

type flag byte

const (
	TaskStarted flag = 1 << iota
	TaskCompleted
	CompTaskStarted
	CompTaskCompleted
)

/*
 * Data Structure representation of the current state of the Saga.
 */
type SagaState struct {
	sagaId string
	job    []byte

	//map of taskId to Flag specifying task progress
	taskState map[string]flag

	//map of taskId to results in EndTask message
	taskResults map[string][]byte

	//map of taskId to reulst returned as part of EndCompTask message
	compTaskResults map[string][]byte

	//bool if AbortSaga message logged
	sagaAborted bool

	//bool if EndSaga message logged
	sagaCompleted bool
}

/*
 * Initialize a Default Empty Saga
 */
func initializeSagaState() *SagaState {
	return &SagaState{
		sagaId:          "",
		job:             nil,
		taskState:       make(map[string]flag),
		taskResults:     make(map[string][]byte),
		compTaskResults: make(map[string][]byte),
		sagaAborted:     false,
		sagaCompleted:   false,
	}
}

/*
 * Returns the Id of the Saga this state represents
 */
func (state *SagaState) SagaId() string {
	return state.sagaId
}

/*
 * Returns the Job associated with this Saga
 */
func (state *SagaState) Job() []byte {
	return state.job
}

/*
 * Returns true if the specified Task has been started,
 * fasle otherwise
 */
func (state *SagaState) IsTaskStarted(taskId string) bool {
	flags, _ := state.taskState[taskId]
	return flags&TaskStarted != 0
}

/*
 * Returns true if the specified Task has been completed,
 * fasle otherwise
 */
func (state *SagaState) IsTaskCompleted(taskId string) bool {
	flags, _ := state.taskState[taskId]
	return flags&TaskCompleted != 0
}

/*
 * Returns true if the specified Compensating Task has been started,
 * fasle otherwise
 */
func (state *SagaState) IsCompTaskStarted(taskId string) bool {
	flags, _ := state.taskState[taskId]
	return flags&CompTaskStarted != 0
}

/*
 * Returns true if the specified Compensating Task has been completed,
 * fasle otherwise
 */
func (state *SagaState) IsCompTaskCompleted(taskId string) bool {
	flags, _ := state.taskState[taskId]
	return flags&CompTaskCompleted != 0
}

/*
 * Returns true if this Saga has been Aborted, false otherwise
 */
func (state *SagaState) IsSagaAborted() bool {
	return state.sagaAborted
}

/*
 * Returns true if this Saga has been Completed, false otherwise
 */
func (state *SagaState) IsSagaCompleted() bool {
	return state.sagaCompleted
}

/*
 * Applies the supplied message to the supplied sagaState.  Does not mutate supplied Saga State
 * Instead returns a new SagaState which has the update applied to it
 *
 * Returns an Error if applying the message would result in an invalid Saga State
 */
func updateSagaState(s *SagaState, msg sagaMessage) (*SagaState, error) {

	//first copy current state, and then apply update so we don't mutate the passed in SagaState
	state := copySagaState(s)

	if msg.sagaId != state.sagaId {
		return nil, fmt.Errorf("InvalidSagaMessage: sagaId %s & SagaMessage sagaId %s do not match", state.sagaId, msg.sagaId)
	}

	switch msg.msgType {

	case EndSaga:

		//A Successfully Completed Saga must have StartTask/EndTask pairs for all messages or
		//an aborted Saga must have StartTask/StartCompTask/EndCompTask pairs for all messages
		for taskId := range state.taskState {

			if state.sagaAborted {
				if !(state.IsCompTaskStarted(taskId) && state.IsCompTaskCompleted(taskId)) {
					return nil, errors.New(fmt.Sprintf("InvalidSagaState: End Saga Message cannot be applied to an aborted Saga where Task %s has not completed its compensating Tasks", taskId))
				}
			} else {
				if !state.IsTaskCompleted(taskId) {
					return nil, errors.New(fmt.Sprintf("InvalidSagaState: End Saga Message cannot be applied to a Saga where Task %s has not completed", taskId))
				}
			}
		}

		state.sagaCompleted = true

	case AbortSaga:
		state.sagaAborted = true

	case StartTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		state.taskState[msg.taskId] = TaskStarted

	case EndTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		// All EndTask Messages must have a preceding StartTask Message
		if !state.IsTaskStarted(msg.taskId) {
			return nil, fmt.Errorf("InvalidSagaState: Cannot have a EndTask %s Message Before a StartTask %s Message", msg.taskId, msg.taskId)
		}

		state.taskState[msg.taskId] = state.taskState[msg.taskId] | TaskCompleted

	case StartCompTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		//In order to apply compensating transactions a saga must first be aborted
		if !state.IsSagaAborted() {
			return nil, fmt.Errorf("InvalidSagaState: Cannot have a StartCompTask %s Message when Saga has not been Aborted", msg.taskId)
		}

		// All StartCompTask Messages must have a preceding StartTask Message
		if !state.IsTaskStarted(msg.taskId) {
			return nil, fmt.Errorf("InvalidSaga State: Cannot have a StartCompTask %s Message Before a StartTask %s Message", msg.taskId, msg.taskId)
		}

		state.taskState[msg.taskId] = state.taskState[msg.taskId] | CompTaskStarted

	case EndCompTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		//in order to apply compensating transactions a saga must first be aborted
		if !state.IsSagaAborted() {
			return nil, fmt.Errorf("InvalidSagaState: Cannot have a EndCompTask %s Message when Saga has not been Aborted", msg.taskId)
		}

		// All EndCompTask Messages must have a preceding StartTask Message
		if !state.IsTaskStarted(msg.taskId) {
			return nil, fmt.Errorf("InvalidSagaState: Cannot have a StartCompTask %s Message Before a StartTask %s Message", msg.taskId, msg.taskId)
		}

		// All EndCompTask Messages must have a preceding StartCompTask Message
		if !state.IsCompTaskStarted(msg.taskId) {
			return nil, fmt.Errorf("InvalidSagaState: Cannot have a EndCompTask %s Message Before a StartCompTaks %s Message", msg.taskId, msg.taskId)
		}

		state.taskState[msg.taskId] = state.taskState[msg.taskId] | CompTaskCompleted
	}

	return state, nil
}

/*
 * Creates a deep copy of mutable saga state.  Does not Deepcopy
 * Job field since this is never mutated after creation
 */
func copySagaState(s *SagaState) *SagaState {

	newS := &SagaState{
		sagaId:        s.sagaId,
		sagaAborted:   s.sagaAborted,
		sagaCompleted: s.sagaCompleted,
	}

	newS.taskState = make(map[string]flag)
	for key, value := range s.taskState {
		newS.taskState[key] = value
	}

	//don't need to deep copy job, since its only set on create.
	newS.job = s.job

	return newS
}

/*
 * Validates that a SagaId Is valid. Returns error if valid, nil otherwise
 */
func validateSagaId(sagaId string) error {
	if sagaId == "" {
		return fmt.Errorf("Invalid Saga Message: sagaId cannot be the empty string")
	} else {
		return nil
	}
}

/*
 * Validates that a TaskId Is valid. Returns error if valid, nil otherwise
 */
func validateTaskId(taskId string) error {
	if taskId == "" {
		return fmt.Errorf("Invalid Saga Message: taskId cannot be the empty string")
	} else {
		return nil
	}
}

/*
 * Initialize a SagaState for the specified saga, and default data.
 */
func sagaStateFactory(sagaId string, job []byte) (*SagaState, error) {

	state := initializeSagaState()

	err := validateSagaId(sagaId)
	if err != nil {
		return nil, err
	}

	state.sagaId = sagaId
	state.job = job

	return state, nil
}
