package saga

import (
	"errors"
	"fmt"
)

/*
 * Additional data about tasks to be committed to the log
 * This is Opaque to SagaState, but useful data to persist for
 * results or debugging
 */
type taskData struct {
	taskStart     []byte
	taskEnd       []byte
	compTaskStart []byte
	compTaskEnd   []byte
}

/*
 * Data Structure representation of the current state of the Saga.
 */
type SagaState struct {
	sagaId string
	job    []byte

	// map of taskID to task data supplied when committing
	// startTask, endTask, startCompTask, endCompTask messages
	taskData map[string]*taskData

	//map of taskId to StartTask message logged
	taskStarted map[string]bool

	//map of taskId to EndTask message logged
	taskCompleted map[string]bool

	//map of taskId to StartCompTask message logged
	compTaskStarted map[string]bool

	//map of taskId to EndCompTask message logged
	compTaskCompleted map[string]bool

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
		compTaskStarted:   make(map[string]bool),
		compTaskCompleted: make(map[string]bool),
		taskData:          make(map[string]*taskData),
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
 * Get Data Associated with Start Task, supplied as
 * Part of the StartTask Message
 */
func (state *SagaState) GetStartTaskData(taskId string) []byte {
	data, ok := state.taskData[taskId]
	if ok {
		return data.taskStart
	} else {
		return nil
	}
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
 * Get Data Associated with End Task, supplied as
 * Part of the EndTask Message
 */
func (state *SagaState) GetEndTaskData(taskId string) []byte {
	data, ok := state.taskData[taskId]
	if ok {
		return data.taskEnd
	} else {
		return nil
	}
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
 * Get Data Associated with Starting Comp Task, supplied as
 * Part of the StartCompTask Message
 */
func (state *SagaState) GetStartCompTaskData(taskId string) []byte {
	data, ok := state.taskData[taskId]
	if ok {
		return data.compTaskStart
	} else {
		return nil
	}
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
 * Get Data Associated with End Comp Task, supplied as
 * Part of the EndCompTask Message
 */
func (state *SagaState) GetEndCompTaskData(taskId string) []byte {
	data, ok := state.taskData[taskId]
	if ok {
		return data.compTaskEnd
	} else {
		return nil
	}
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
 * Add the data for the specified message type to the task metadata fields.
 * This Data is stored in the SagaState and persisted to durable saga log, so it
 * can be recovered.  It is opaque to sagas but useful to persist for applications.
 */
func (state *SagaState) addTaskData(taskId string, msgType SagaMessageType, data []byte) {

	tData, ok := state.taskData[taskId]
	if !ok {
		tData = &taskData{}
		state.taskData[taskId] = tData
	}

	switch msgType {
	case StartTask:
		state.taskData[taskId].taskStart = data

	case EndTask:
		state.taskData[taskId].taskEnd = data

	case StartCompTask:
		state.taskData[taskId].compTaskStart = data

	case EndCompTask:
		state.taskData[taskId].compTaskEnd = data
	}
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
		if msg.data != nil {
			state.addTaskData(msg.taskId, msg.msgType, msg.data)
		}

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

		if msg.data != nil {
			state.addTaskData(msg.taskId, msg.msgType, msg.data)
		}

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

		if msg.data != nil {
			state.addTaskData(msg.taskId, msg.msgType, msg.data)
		}

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

		if msg.data != nil {
			state.addTaskData(msg.taskId, msg.msgType, msg.data)
		}
	}

	return state, nil
}

/*
 * Creates a copy of mutable saga state.  Does not copy
 * binary data, only pointers to it.
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

	newS.taskData = make(map[string]*taskData)
	for key, value := range s.taskData {
		newS.taskData[key] = &taskData{
			taskStart:     value.taskStart,
			taskEnd:       value.taskEnd,
			compTaskStart: value.compTaskStart,
			compTaskEnd:   value.compTaskEnd,
		}
	}

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
