package saga

import "errors"
import "fmt"

/*
 * Data Structure representation of the current state of the Saga.
 */
type SagaState struct {
	sagaId string
	job    []byte

	taskStarted   map[string]bool
	taskCompleted map[string]bool
	taskResults   map[string][]byte

	compTaskStarted   map[string]bool
	compTaskCompleted map[string]bool
	compTaskResults   map[string][]byte

	sagaAborted   bool
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
func (s *SagaState) SagaId() string {
	return s.sagaId
}

/*
 * Returns the Job associated with this Saga
 */
func (s *SagaState) Job() []byte {
	return s.job
}

/*
 * Returns true if the specified Task has been started,
 * fasle otherwise
 */
func (s *SagaState) IsTaskStarted(taskId string) bool {
	started, ok := s.taskStarted[taskId]
	return started && ok
}

/*
 * Returns true if the specified Task has been completed,
 * fasle otherwise
 */
func (s *SagaState) IsTaskCompleted(taskId string) bool {
	completed, ok := s.taskCompleted[taskId]
	return completed && ok
}

/*
 * Returns true if the specified Compensating Task has been started,
 * fasle otherwise
 */
func (s *SagaState) IsCompTaskStarted(taskId string) bool {
	started, ok := s.compTaskStarted[taskId]
	return started && ok
}

/*
 * Returns true if the specified Compensating Task has been completed,
 * fasle otherwise
 */
func (s *SagaState) IsCompTaskCompleted(taskId string) bool {
	completed, ok := s.compTaskCompleted[taskId]
	return completed && ok
}

/*
 * Returns true if this Saga has been Aborted, false otherwise
 */
func (s *SagaState) IsSagaAborted() bool {
	return s.sagaAborted
}

/*
 * Returns true if this Saga has been Completed, false otherwise
 */
func (s *SagaState) IsSagaCompleted() bool {
	return s.sagaCompleted
}

/*
 * Updates a SagaState with the specified SagaMessage.
 * Returns an InvalidSagaState Error if applying the message would result in an invalid Saga State
 * Returns an InvalidSagaMessage Error if the message is Invalid
 */
func (state *SagaState) UpdateSagaState(msg sagaMessage) error {
	if msg.sagaId != state.sagaId {
		return errors.New(fmt.Sprintf("InvalidSagaState: sagaId %s & SagaMessage sagaId %s do not match", state.sagaId, msg.sagaId))
	}

	switch msg.msgType {

	case StartSaga:
		state.job = msg.data

	case EndSaga:

		//A Successfully Completed Saga must have StartTask/EndTask pairs for all messages or
		//an aborted Saga must have StartTask/StartCompTask/EndCompTask pairs for all messages
		for taskId := range state.taskStarted {

			if state.sagaAborted {
				if !(state.compTaskStarted[taskId] && state.compTaskCompleted[taskId]) {
					return errors.New(fmt.Sprintf("InvalidSagaState: End Saga Message cannot be applied to an aborted Saga where Task %s has not completed its compensating Tasks", taskId))
				}
			} else {
				if !state.taskCompleted[taskId] {
					return errors.New(fmt.Sprintf("InvalidSagaState: End Saga Message cannot be applied to a Saga where Task %s has not completed", taskId))
				}
			}
		}

		state.sagaCompleted = true

	case AbortSaga:
		state.sagaAborted = true

	case StartTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return err
		}

		state.taskStarted[msg.taskId] = true

	case EndTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return err
		}

		// All EndTask Messages must have a preceding StartTask Message
		if !state.taskStarted[msg.taskId] {
			return errors.New(fmt.Sprintf("Invalid Saga State: Cannot have a EndTask %s Message Before a StartTask %s Message", msg.taskId, msg.taskId))
		}

		state.taskCompleted[msg.taskId] = true

	case StartCompTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return err
		}

		//In order to apply compensating transactions a saga must first be aborted
		if !state.sagaAborted {
			return errors.New(fmt.Sprintf("Invalid SagaState: Cannot have a StartCompTask %s Message when Saga has not been Aborted", msg.taskId))
		}

		// All StartCompTask Messages must have a preceding StartTask Message
		if !state.taskStarted[msg.taskId] {
			return errors.New(fmt.Sprintf("Invalid Saga State: Cannot have a StartCompTask %s Message Before a StartTask %s Message", msg.taskId, msg.taskId))
		}

		state.compTaskStarted[msg.taskId] = true

	case EndCompTask:
		err := validateTaskId(msg.taskId)
		if err != nil {
			return err
		}

		//in order to apply compensating transactions a saga must first be aborted
		if !state.sagaAborted {
			return errors.New(fmt.Sprintf("Invalid SagaState: Cannot have a EndCompTask %s Message when Saga has not been Aborted", msg.taskId))
		}

		// All EndCompTask Messages must have a preceding StartTask Message
		if !state.taskStarted[msg.taskId] {
			return errors.New(fmt.Sprintf("Invalid Saga State: Cannot have a StartCompTask %s Message Before a StartTask %s Message", msg.taskId, msg.taskId))
		}

		// All EndCompTask Messages must have a preceding StartCompTask Message
		if !state.compTaskStarted[msg.taskId] {
			return errors.New(fmt.Sprintf("Invalid Saga State: Cannot have a EndCompTask %s Message Before a StartCompTaks %s Message", msg.taskId, msg.taskId))
		}

		state.compTaskCompleted[msg.taskId] = true
	}

	return nil
}

/*
 * Validates that a SagaId Is valid. Returns error if valid, nil otherwise
 */
func validateSagaId(sagaId string) error {
	if sagaId == "" {
		return errors.New(fmt.Sprint("Invalid Saga Message: sagaId cannot be the empty string"))
	} else {
		return nil
	}
}

/*
 * Validates that a TaskId Is valid. Returns error if valid, nil otherwise
 */
func validateTaskId(taskId string) error {
	if taskId == "" {
		return errors.New(fmt.Sprint("Invalid Saga Message: taskId cannot be the empty string"))
	} else {
		return nil
	}
}

/*
 * Initialize a SagaState for the specified saga, and default data.
 */
func SagaStateFactory(sagaId string, job []byte) (*SagaState, error) {

	state := initializeSagaState()

	err := validateSagaId(sagaId)
	if err != nil {
		return nil, err
	}

	state.sagaId = sagaId
	state.job = job

	return state, nil
}
