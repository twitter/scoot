package saga

import (
	"errors"
	"fmt"
)

/*
 * Data Structure representation of the current state of the Saga.
 */
type SagaState struct {
	sagaId string
	job    []byte

	//map of taskId to StartTask message logged
	taskStarted map[string]bool

	//map of taskId to EndTask message logged
	taskCompleted map[string]bool

	//map of taskId to results in EndTask message
	taskResults map[string][]byte

	//map of taskId to StartCompTask message logged
	compTaskStarted map[string]bool

	//map of taskId to EndCompTask message logged
	compTaskCompleted map[string]bool

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
		sagaId:            "",
		job:               nil,
		taskStarted:       make(map[string]bool),
		taskCompleted:     make(map[string]bool),
		taskResults:       make(map[string][]byte),
		compTaskStarted:   make(map[string]bool),
		compTaskCompleted: make(map[string]bool),
		compTaskResults:   make(map[string][]byte),
		sagaAborted:       false,
		sagaCompleted:     false,
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
	started, _ := state.taskStarted[taskId]
	return started
}

/*
 * Returns true if the specified Task has been completed,
 * fasle otherwise
 */
func (state *SagaState) IsTaskCompleted(taskId string) bool {
	completed, _ := state.taskCompleted[taskId]
	return completed
}

/*
 * Returns true if the specified Compensating Task has been started,
 * fasle otherwise
 */
func (state *SagaState) IsCompTaskStarted(taskId string) bool {
	started, _ := state.compTaskStarted[taskId]
	return started
}

/*
 * Returns true if the specified Compensating Task has been completed,
 * fasle otherwise
 */
func (state *SagaState) IsCompTaskCompleted(taskId string) bool {
	completed, _ := state.compTaskCompleted[taskId]
	return completed
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
 * Instead returns a new SagaState which has the update applied
 *
 * Returns an InvalidSagaState Error if applying the message would result in an invalid Saga State
 * Returns an InvalidSagaMessage Error if the message is Invalid
 */
func updateSagaState(s *SagaState, msg sagaMessage) (*SagaState, error) {

	//first copy current state, and then apply update so we don't mutate the passed in SagaState
	state := copySagaState(s)

	if msg.sagaId != state.sagaId {
		return nil, fmt.Errorf("InvalidSagaState: sagaId %s & SagaMessage sagaId %s do not match", state.sagaId, msg.sagaId)
	}

	switch msg.msgType {

	case EndSaga:

		//A Successfully Completed Saga must have StartTask/EndTask pairs for all messages or
		//an aborted Saga must have StartTask/StartCompTask/EndCompTask pairs for all messages
		for taskId := range state.taskStarted {

			if state.sagaAborted {
				if !(state.compTaskStarted[taskId] && state.compTaskCompleted[taskId]) {
					return nil, errors.New(fmt.Sprintf("InvalidSagaState: End Saga Message cannot be applied to an aborted Saga where Task %s has not completed its compensating Tasks", taskId))
				}
			} else {
				if !state.taskCompleted[taskId] {
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

		state.taskStarted[msg.taskId] = true

	case EndTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		// All EndTask Messages must have a preceding StartTask Message
		if !state.taskStarted[msg.taskId] {
			return nil, fmt.Errorf("Invalid Saga State: Cannot have a EndTask %s Message Before a StartTask %s Message", msg.taskId, msg.taskId)
		}

		state.taskCompleted[msg.taskId] = true

	case StartCompTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		//In order to apply compensating transactions a saga must first be aborted
		if !state.sagaAborted {
			return nil, fmt.Errorf("Invalid SagaState: Cannot have a StartCompTask %s Message when Saga has not been Aborted", msg.taskId)
		}

		// All StartCompTask Messages must have a preceding StartTask Message
		if !state.taskStarted[msg.taskId] {
			return nil, fmt.Errorf("Invalid Saga State: Cannot have a StartCompTask %s Message Before a StartTask %s Message", msg.taskId, msg.taskId)
		}

		state.compTaskStarted[msg.taskId] = true

	case EndCompTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		//in order to apply compensating transactions a saga must first be aborted
		if !state.sagaAborted {
			return nil, fmt.Errorf("Invalid SagaState: Cannot have a EndCompTask %s Message when Saga has not been Aborted", msg.taskId)
		}

		// All EndCompTask Messages must have a preceding StartTask Message
		if !state.taskStarted[msg.taskId] {
			return nil, fmt.Errorf("Invalid Saga State: Cannot have a StartCompTask %s Message Before a StartTask %s Message", msg.taskId, msg.taskId)
		}

		// All EndCompTask Messages must have a preceding StartCompTask Message
		if !state.compTaskStarted[msg.taskId] {
			return nil, fmt.Errorf("Invalid Saga State: Cannot have a EndCompTask %s Message Before a StartCompTaks %s Message", msg.taskId, msg.taskId)
		}

		state.compTaskCompleted[msg.taskId] = true
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

	newS.taskStarted = make(map[string]bool)
	for key, value := range s.taskStarted {
		newS.taskStarted[key] = value
	}

	newS.taskCompleted = make(map[string]bool)
	for key, value := range s.taskCompleted {
		newS.taskCompleted[key] = value
	}

	newS.compTaskStarted = make(map[string]bool)
	for key, value := range s.compTaskStarted {
		newS.compTaskStarted[key] = value
	}

	newS.compTaskCompleted = make(map[string]bool)
	for key, value := range s.compTaskCompleted {
		newS.compTaskCompleted[key] = value
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
