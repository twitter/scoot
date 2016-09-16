package saga

import (
	"fmt"
)

type InvalidSagaStateError struct {
	s string
}

func (e InvalidSagaStateError) Error() string {
	return e.s
}

func NewInvalidSagaStateError(msg string, args ...interface{}) error {
	return InvalidSagaStateError{
		s: fmt.Sprintf(msg, args...),
	}
}

type InvalidSagaMessageError struct {
	s string
}

func (e InvalidSagaMessageError) Error() string {
	return e.s
}

func NewInvalidSagaMessageError(msg string) error {
	return InvalidSagaMessageError{
		s: msg,
	}
}

type flag byte

const (
	TaskStarted flag = 1 << iota
	TaskCompleted
	CompTaskStarted
	CompTaskCompleted
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

	// map of taskId to Flag specifying task progress
	taskState map[string]flag

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
		sagaId:        "",
		job:           nil,
		taskState:     make(map[string]flag),
		taskData:      make(map[string]*taskData),
		sagaAborted:   false,
		sagaCompleted: false,
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
 * Returns a lists of task ids associated with this Saga
 */
func (state *SagaState) GetTaskIds() []string {
	taskIds := make([]string, 0, len(state.taskState))

	for id, _ := range state.taskState {
		taskIds = append(taskIds, id)
	}

	return taskIds
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
	flags, _ := state.taskState[taskId]
	return flags&TaskCompleted != 0
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
	flags, _ := state.taskState[taskId]
	return flags&CompTaskStarted != 0
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
	flags, _ := state.taskState[taskId]
	return flags&CompTaskCompleted != 0
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
 * Instead returns a new SagaState which has the update applied to it
 *
 * Returns an Error if applying the message would result in an invalid Saga State
 */
func updateSagaState(s *SagaState, msg sagaMessage) (*SagaState, error) {

	//first copy current state, and then apply update so we don't mutate the passed in SagaState
	state := copySagaState(s)

	if msg.sagaId != state.sagaId {
		return nil, NewInvalidSagaMessageError(fmt.Sprintf("sagaId %s & SagaMessage sagaId %s do not match", state.sagaId, msg.sagaId))
	}

	switch msg.msgType {

	case StartSaga:
		return nil, NewInvalidSagaStateError("Cannot apply a StartSaga Message to an already existing Saga")

	case EndSaga:

		//A Successfully Completed Saga must have StartTask/EndTask pairs for all messages or
		//an aborted Saga must have StartTask/StartCompTask/EndCompTask pairs for all messages
		for taskId := range state.taskState {

			if state.sagaAborted {
				if !(state.IsCompTaskStarted(taskId) && state.IsCompTaskCompleted(taskId)) {
					return nil, NewInvalidSagaStateError(fmt.Sprintf("End Saga Message cannot be applied to an aborted Saga where Task %s has not completed its compensating Tasks", taskId))
				}
			} else {
				if !state.IsTaskCompleted(taskId) {
					return nil, NewInvalidSagaStateError(fmt.Sprintf("End Saga Message cannot be applied to a Saga where Task %s has not completed", taskId))
				}
			}
		}

		state.sagaCompleted = true

	case AbortSaga:

		if state.IsSagaCompleted() {
			return nil, NewInvalidSagaStateError("AbortSaga Message cannot be applied to a Completed Saga")
		}

		state.sagaAborted = true

	case StartTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		if state.IsSagaCompleted() {
			return nil, NewInvalidSagaStateError("Cannot StartTask after Saga has been completed: %s", msg.taskId)
		}

		if state.IsSagaAborted() {
			return nil, NewInvalidSagaStateError("Cannot StartTask after Saga has been aborted")
		}

		if state.IsTaskCompleted(msg.taskId) {
			return nil, NewInvalidSagaStateError("Cannot StartTask after it has been completed: %s", msg.taskId)
		}

		if msg.data != nil {
			state.addTaskData(msg.taskId, msg.msgType, msg.data)
		}

		state.taskState[msg.taskId] = TaskStarted

	case EndTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		if state.IsSagaCompleted() {
			return nil, NewInvalidSagaStateError("Cannot EndTask after Saga has been completed")
		}

		if state.IsSagaAborted() {
			return nil, NewInvalidSagaStateError("Cannot EndTask after an Abort Saga Message")
		}

		// All EndTask Messages must have a preceding StartTask Message
		if !state.IsTaskStarted(msg.taskId) {
			return nil, NewInvalidSagaStateError(fmt.Sprintf("Cannot have a EndTask Message Before a StartTask Message, taskId: %s", msg.taskId))
		}

		state.taskState[msg.taskId] = state.taskState[msg.taskId] | TaskCompleted

		if msg.data != nil {
			state.addTaskData(msg.taskId, msg.msgType, msg.data)
		}

	case StartCompTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		if state.IsSagaCompleted() {
			return nil, NewInvalidSagaStateError("Cannot StartCompTask after Saga has been completed")
		}

		//In order to apply compensating transactions a saga must first be aborted
		if !state.IsSagaAborted() {
			return nil, NewInvalidSagaStateError(fmt.Sprintf("Cannot have a StartCompTask Message when Saga has not been Aborted, taskId: %s", msg.taskId))
		}

		// All StartCompTask Messages must have a preceding StartTask Message
		if !state.IsTaskStarted(msg.taskId) {
			return nil, NewInvalidSagaStateError(fmt.Sprintf("Cannot have a StartCompTask Message Before a StartTask Message, taskId: %s", msg.taskId))
		}

		if state.IsCompTaskCompleted(msg.taskId) {
			return nil, NewInvalidSagaStateError(fmt.Sprintf("Cannot StartCompTask after it has been completed, taskId: %s", msg.taskId))
		}

		state.taskState[msg.taskId] = state.taskState[msg.taskId] | CompTaskStarted

		if msg.data != nil {
			state.addTaskData(msg.taskId, msg.msgType, msg.data)
		}

	case EndCompTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return nil, err
		}

		if state.IsSagaCompleted() {
			return nil, NewInvalidSagaStateError("Cannot EndCompTask after Saga has been completed")
		}

		//in order to apply compensating transactions a saga must first be aborted
		if !state.IsSagaAborted() {
			return nil, NewInvalidSagaStateError(fmt.Sprintf("Cannot have a EndCompTask Message when Saga has not been Aborted, taskId: %s", msg.taskId))
		}

		// All EndCompTask Messages must have a preceding StartTask Message
		if !state.IsTaskStarted(msg.taskId) {
			return nil, NewInvalidSagaStateError(fmt.Sprintf("Cannot have a StartCompTask Message Before a StartTask Message, taskId: %s", msg.taskId))
		}

		// All EndCompTask Messages must have a preceding StartCompTask Message
		if !state.IsCompTaskStarted(msg.taskId) {
			return nil, NewInvalidSagaStateError(fmt.Sprintf("Cannot have a EndCompTask Message Before a StartCompTaks Message, taskId: %s", msg.taskId))
		}

		if msg.data != nil {
			state.addTaskData(msg.taskId, msg.msgType, msg.data)
		}

		state.taskState[msg.taskId] = state.taskState[msg.taskId] | CompTaskCompleted

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

	newS.taskState = make(map[string]flag)
	for key, value := range s.taskState {
		newS.taskState[key] = value
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
		return NewInvalidSagaMessageError("sagaId cannot be the empty string")
	} else {
		return nil
	}
}

/*
 * Validates that a TaskId Is valid. Returns error if valid, nil otherwise
 */
func validateTaskId(taskId string) error {
	if taskId == "" {
		return NewInvalidSagaMessageError("taskId cannot be the empty string")
	} else {
		return nil
	}
}

/*
 * Initialize a SagaState for the specified saga, and default data.
 */
func makeSagaState(sagaId string, job []byte) (*SagaState, error) {

	state := initializeSagaState()

	err := validateSagaId(sagaId)
	if err != nil {
		return nil, err
	}

	state.sagaId = sagaId
	state.job = job

	return state, nil
}

/*
 * Custom ToString function for SagaState
 */
func (state *SagaState) String() string {

	fmtString := "{ SagaId: %v, " +
		"SagaAborted: %v, " +
		"SagaCompleted: %v, " +
		"Tasks: [ "

	for _, id := range state.GetTaskIds() {

		taskState := ""

		if state.IsTaskStarted(id) {
			taskState += "Started|"
		}
		if state.IsTaskCompleted(id) {
			taskState += "Completed|"
		}
		if state.IsCompTaskStarted(id) {
			taskState += "CompTaskStarted|"
		}
		if state.IsCompTaskCompleted(id) {
			taskState += "CompTaskCompleted|"
		}

		// remove trailing slash
		if len(taskState) >= 1 {
			taskState = taskState[0 : len(taskState)-1]
		}

		fmtString += fmt.Sprintf("%v: %s, ", id, taskState)
	}

	fmtString += "]"

	return fmt.Sprintf(
		fmtString,
		state.sagaId,
		state.IsSagaAborted(),
		state.IsSagaCompleted(),
	)
}
