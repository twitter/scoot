// Package saga provides a generic implementation of the Saga pattern.
// Scoot uses the Saga pattern to track the state of long-lived Jobs
// for reporting and failure recovery.
// For info on the Saga pattern, see:
// https://speakerdeck.com/caitiem20/applying-the-saga-pattern
package saga

import (
	"sync"
)

// Concurrent Object Representing a Saga
// Methods update the state of the saga or
// Provide access to the Current State
type Saga struct {
	id       string
	log      SagaLog
	state    *SagaState
	updateCh chan sagaUpdate
	mutex    sync.RWMutex
}

// Start a New Saga.  Logs a Start Saga Message to the SagaLog
// returns a Saga, or an error if one occurs
func newSaga(sagaId string, job []byte, log SagaLog) (*Saga, error) {

	state, err := makeSagaState(sagaId, job)
	if err != nil {
		return nil, err
	}

	err = log.StartSaga(sagaId, job)
	if err != nil {
		return nil, err
	}

	updateCh := make(chan sagaUpdate, 0)

	s := &Saga{
		id:       sagaId,
		log:      log,
		state:    state,
		updateCh: updateCh,
		mutex:    sync.RWMutex{},
	}

	go s.updateSagaStateLoop()

	return s, nil
}

// Rehydrate a saga from a specified SagaState, does not write
// to SagaLog assumes that this is a recovered saga.
func rehydrateSaga(sagaId string, state *SagaState, log SagaLog) *Saga {
	updateCh := make(chan sagaUpdate, 0)
	s := &Saga{
		id:       sagaId,
		log:      log,
		state:    state,
		updateCh: updateCh,
		mutex:    sync.RWMutex{},
	}

	if !state.IsSagaCompleted() {
		go s.updateSagaStateLoop()
	}

	return s
}

// Returns the Current Saga State
func (s *Saga) GetState() *SagaState {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

//
// Log an End Saga Message to the log, returns updated SagaState
// Returns the resulting SagaState or an error if it fails
//
// Once EndSaga is succesfully called, trying to log additional
// messages will result in a panic.
func (s *Saga) EndSaga() error {
	return s.updateSagaState(MakeEndSagaMessage(s.id))
}

//
// Log an AbortSaga message.  This indicates that the
// Saga has failed and all execution should be stopped
// and compensating transactions should be applied.
//
// Returns an error if it fails
//
func (s *Saga) AbortSaga() error {
	return s.updateSagaState(MakeAbortSagaMessage(s.id))
}

//
// Log a StartTask Message to the log.  Returns
// an error if it fails.
//
// StartTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written StartTask message will win
//
// Returns an error if it fails
//
func (s *Saga) StartTask(taskId string, data []byte) error {
	return s.updateSagaState(MakeStartTaskMessage(s.id, taskId, data))
}

//
// Log an EndTask Message to the log.  Indicates that this task
// has been successfully completed. Returns an error if it fails.
//
// EndTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written EndTask message will win
//
// Returns an error if it fails
//
func (s *Saga) EndTask(taskId string, results []byte) error {
	return s.updateSagaState(MakeEndTaskMessage(s.id, taskId, results))
}

//
// Log a Start Compensating Task Message to the log. Should only be logged after a Saga
// has been avoided and in Rollback Recovery Mode. Should not be used in ForwardRecovery Mode
// returns an error if it fails
//
// StartCompTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written StartCompTask message will win
//
// Returns an error if it fails
//
func (s *Saga) StartCompensatingTask(taskId string, data []byte) error {
	return s.updateSagaState(MakeStartCompTaskMessage(s.id, taskId, data))
}

//
// Log an End Compensating Task Message to the log when a Compensating Task
// has been successfully completed. Returns an error if it fails.
//
// EndCompTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written EndCompTask message will win
//
// Returns an error if it fails
//
func (s *Saga) EndCompensatingTask(taskId string, results []byte) error {
	return s.updateSagaState(MakeEndCompTaskMessage(s.id, taskId, results))
}

// adds a message for updateSagaStateLoop to execute to the channel for the
// specified saga.  blocks until the message has been applied
func (s *Saga) updateSagaState(msg SagaMessage) error {
	resultCh := make(chan error, 0)
	s.updateCh <- sagaUpdate{
		msg:      msg,
		resultCh: resultCh,
	}

	result := <-resultCh

	// after we successfully log an EndSaga message close the channel
	// no more messages should be logged
	if msg.MsgType == EndSaga {
		close(s.updateCh)
	}

	return result
}

// updateSagaStateLoop that is executed inside of a single go routine.  There
// is one per saga currently executing.  This ensures all updates are applied
// in order to a saga.  Also controls access to the SagaState so its is
// updated in a thread safe manner
func (s *Saga) updateSagaStateLoop() {
	for update := range s.updateCh {
		s.itr(update)
	}
}

func (s *Saga) itr(update sagaUpdate) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	currState := s.state
	newState, err := logMessage(currState, update.msg, s.log)

	if err == nil {
		s.state = newState

		update.resultCh <- nil

	} else {
		update.resultCh <- err
	}
}

type sagaUpdate struct {
	msg      SagaMessage
	resultCh chan error
}

//
// logs the specified message durably to the SagaLog & updates internal state if its a valid state transition
//
func logMessage(state *SagaState, msg SagaMessage, log SagaLog) (*SagaState, error) {

	//verify that the applied message results in a valid state
	newState, err := updateSagaState(state, msg)
	if err != nil {
		return nil, err
	}

	//try durably storing the message
	err = log.LogMessage(msg)
	if err != nil {
		return nil, err
	}

	return newState, nil
}

// Checks the error returned by updating saga state.
// Returns true if the error is a FatalErr.
// Returns false if the error is transient and a retry might succeed
func FatalErr(err error) bool {

	switch err.(type) {
	// InvalidSagaState is an unrecoverable error. This indicates a fatal bug in the code
	// which is asking for an impossible transition.
	case InvalidSagaStateError:
		return true

	// InvalidSagaMessage is an unrecoverable error.  This indicates a fatal bug in the code
	// which is applying invalid parameters to a saga.
	case InvalidSagaMessageError:
		return true

	// InvalidRequestError is an unrecoverable error.  This indicates a fatal bug in the code
	// where the SagaLog cannot durably store messages
	case InvalidRequestError:
		return true

	// InternalLogError is a transient error experienced by the log.  It was not
	// able to durably store the message but it is ok to retry.
	case InternalLogError:
		return false

	// SagaLog is in a bad state, this indicates a fatal bug and no more progress
	// can be made on this saga.
	case CorruptedSagaLogError:
		return true

	// unknown error, default to retryable.
	default:
		return true
	}
}
