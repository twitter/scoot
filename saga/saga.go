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
	mutex    sync.RWMutex // mutex controls access to Saga.state
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

func (s *Saga) ID() string {
	return s.id
}

// Returns the Current Saga State
func (s *Saga) GetState() *SagaState {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return copySagaState(s.state)
}

// Log an End Saga Message to the log, returns updated SagaState
// Returns the resulting SagaState or an error if it fails
//
// Once EndSaga is successfully called, trying to log additional
// messages will result in a panic.
func (s *Saga) EndSaga() error {
	return s.updateSagaState([]SagaMessage{MakeEndSagaMessage(s.id)})
}

// Log an AbortSaga message.  This indicates that the
// Saga has failed and all execution should be stopped
// and compensating transactions should be applied.
//
// Returns an error if it fails
//
func (s *Saga) AbortSaga() error {
	return s.updateSagaState([]SagaMessage{MakeAbortSagaMessage(s.id)})
}

// Log a StartTask Message to the log.  Returns
// an error if it fails.
//
// StartTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written StartTask message will win
//
// Returns an error if it fails
//
func (s *Saga) StartTask(taskId string, data []byte) error {
	return s.updateSagaState([]SagaMessage{MakeStartTaskMessage(s.id, taskId, data)})
}

// Log an EndTask Message to the log.  Indicates that this task
// has been successfully completed. Returns an error if it fails.
//
// EndTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written EndTask message will win
//
// Returns an error if it fails
//
func (s *Saga) EndTask(taskId string, results []byte) error {
	return s.updateSagaState([]SagaMessage{MakeEndTaskMessage(s.id, taskId, results)})
}

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
	return s.updateSagaState([]SagaMessage{MakeStartCompTaskMessage(s.id, taskId, data)})
}

// Log an End Compensating Task Message to the log when a Compensating Task
// has been successfully completed. Returns an error if it fails.
//
// EndCompTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written EndCompTask message will win
//
// Returns an error if it fails
//
func (s *Saga) EndCompensatingTask(taskId string, results []byte) error {
	return s.updateSagaState([]SagaMessage{MakeEndCompTaskMessage(s.id, taskId, results)})
}

// BulkMessage takes a slice of SagaMessages to be applied.
// The messages update the saga state and log in the order given.
// The update is done "atomically", within a single
// Saga mutex lock. Note that the underlying log update will be
// SagaLog implementation dependent.
func (s *Saga) BulkMessage(messages []SagaMessage) error {
	return s.updateSagaState(messages)
}

// adds a message for updateSagaStateLoop to execute to the channel for the
// specified saga.  blocks until the message has been applied
func (s *Saga) updateSagaState(msgs []SagaMessage) error {
	resultCh := make(chan error, 0)
	s.updateCh <- sagaUpdate{
		msgs:     msgs,
		resultCh: resultCh,
	}

	result := <-resultCh

	// after we successfully log an EndSaga message close the channel
	// no more messages should be logged
	for _, msg := range msgs {
		if msg.MsgType == EndSaga {
			close(s.updateCh)
			break
		}
	}

	return result
}

// updateSagaStateLoop that is executed inside of a single go routine.  There
// is one per saga currently executing.  This ensures all updates are applied
// in order to a saga.  Also controls access to the SagaState so its is
// updated in a thread safe manner
func (s *Saga) updateSagaStateLoop() {
	for update := range s.updateCh {
		s.updateSaga(update)
	}
}

// updateSaga updates the saga s by applying update atomically and sending any error to the requester
func (s *Saga) updateSaga(update sagaUpdate) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var err error
	s.state, err = logMessages(s.state, update.msgs, s.log)
	update.resultCh <- err
}

type sagaUpdate struct {
	msgs     []SagaMessage
	resultCh chan error
}

// checks the message is a valid transition and logs the specified message durably to the SagaLog
// if msg is an invalid transition, it will neither log nor update internal state
// always returns the new SagaState that should be used, either the mutated one or a copy of the
// original.
func logMessages(state *SagaState, msgs []SagaMessage, log SagaLog) (*SagaState, error) {
	// updateSagaState will mutate state if it's a valid transition, but if we then error storing,
	// we'll need to revert to the old state.
	oldState := copySagaState(state)
	// verify that the applied message results in a valid state
	var err error
	for _, msg := range msgs {
		err = updateSagaState(state, msg)
		if err != nil {
			return oldState, err
		}
	}

	// try durably storing the message
	if len(msgs) == 1 {
		err = log.LogMessage(msgs[0])
	} else {
		err = log.LogBatchMessages(msgs)
	}
	if err != nil {
		return oldState, err
	}

	return state, nil
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
