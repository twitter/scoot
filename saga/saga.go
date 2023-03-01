// Package saga provides a generic implementation of the Saga pattern.
// Scoot uses the Saga pattern to track the state of long-lived Jobs
// for reporting and failure recovery.
// For info on the Saga pattern, see:
// https://speakerdeck.com/caitiem20/applying-the-saga-pattern
package saga

import (
	"fmt"
	"sync"

	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/common/stats"
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
	chMutex  sync.RWMutex // controls send and close of channel
	closed   bool
	stat     stats.StatsReceiver
}

// Start a New Saga.  Logs a Start Saga Message to the SagaLog
// returns a Saga, or an error if one occurs
func newSaga(sagaId string, job []byte, log SagaLog, stat stats.StatsReceiver) (*Saga, error) {
	state, err := makeSagaState(sagaId, job)
	if err != nil {
		return nil, err
	}

	err = log.StartSaga(sagaId, job)
	if err != nil {
		return nil, err
	}

	updateCh := make(chan sagaUpdate, common.DefaultSagaUpdateChSize)

	s := &Saga{
		id:       sagaId,
		log:      log,
		state:    state,
		updateCh: updateCh,
		mutex:    sync.RWMutex{},
		chMutex:  sync.RWMutex{},
		stat:     stat,
	}

	go s.updateSagaStateLoop()

	return s, nil
}

// Rehydrate a saga from a specified SagaState, does not write
// to SagaLog assumes that this is a recovered saga.
func rehydrateSaga(sagaId string, state *SagaState, log SagaLog, stat stats.StatsReceiver) *Saga {
	updateCh := make(chan sagaUpdate, common.DefaultSagaUpdateChSize)
	s := &Saga{
		id:       sagaId,
		log:      log,
		state:    state,
		updateCh: updateCh,
		mutex:    sync.RWMutex{},
		chMutex:  sync.RWMutex{},
		stat:     stat,
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
func (s *Saga) StartTask(taskId string, data []byte) error {
	defer s.stat.Latency(stats.SagaStartOrEndTaskLatency_ms).Time().Stop()
	return s.updateSagaState([]SagaMessage{MakeStartTaskMessage(s.id, taskId, data)})
}

// Log an EndTask Message to the log.  Indicates that this task
// has been successfully completed. Returns an error if it fails.
//
// EndTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written EndTask message will win
//
// Returns an error if it fails
func (s *Saga) EndTask(taskId string, results []byte) error {
	defer s.stat.Latency(stats.SagaStartOrEndTaskLatency_ms).Time().Stop()
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
	s.chMutex.RLock()
	if s.closed {
		return fmt.Errorf("Trying to update saga %s that has closed channel", s.id)
	}
	s.updateCh <- sagaUpdate{
		msgs:     msgs,
		resultCh: resultCh,
	}
	s.chMutex.RUnlock()

	result := <-resultCh

	// after we successfully log an EndSaga message close the channel
	// no more messages should be logged
	for _, msg := range msgs {
		if msg.MsgType == EndSaga {
			s.chMutex.Lock()
			close(s.updateCh)
			s.closed = true
			s.chMutex.Unlock()
			break
		}
	}

	return result
}

// updateSagaStateLoop that is executed inside of a single go routine.  There
// is one per saga currently executing.  This ensures all updates are applied
// in order to a saga.  Also controls access to the SagaState so its is
// updated in a thread safe manner
// Processes at most SagaUpdateChSize number of updates at a time
func (s *Saga) updateSagaStateLoop() {
	var loopLatency = s.stat.Latency(stats.SagaUpdateStateLoopLatency_ms).Time()
	updates := []sagaUpdate{}

	for update := range s.updateCh {
		updates = append(updates, update)
		if len(s.updateCh) == 0 || len(updates) == common.DefaultSagaUpdateChSize {
			loopLatency.Stop()
			s.stat.Histogram(stats.SagaNumUpdatesProcessed).Update(int64(len(updates)))

			s.updateSaga(updates)

			updates = []sagaUpdate{}
			loopLatency.Time()
		}
	}
}

// updateSaga updates the saga s by applying updates atomically and sending any error to the requester
// it checks if the message(s) is a valid transition and batch logs the messages from all updates durably to the SagaLog
// if msg is an invalid transition, it will neither log nor update internal state
// always returns the new SagaState that should be used, either the mutated one or a copy of the original.
func (s *Saga) updateSaga(updates []sagaUpdate) {
	defer s.stat.Latency(stats.SagaUpdateStateLatency_ms).Time().Stop()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// updateSaga will mutate state if it's a valid transition, but if we then error logging,
	// we'll need to revert to old state
	oldState := copySagaState(s.state)

	var err error
	var validUpdates []sagaUpdate
	var validUpdateMsgs []SagaMessage

	for _, update := range updates {
		err = nil

		// verify that the applied message(s) results in a valid state
		if len(update.msgs) == 1 {
			err = updateSagaState(s.state, update.msgs[0])
		} else {
			s.state, err = bulkUpdateSagaState(s.state, update.msgs)
		}

		// don't log messages for invalid updates
		if err != nil {
			update.resultCh <- err
			continue
		}

		validUpdates = append(validUpdates, update)
		validUpdateMsgs = append(validUpdateMsgs, update.msgs...)
	}

	// batch log valid messages(if present) from all updates
	if len(validUpdateMsgs) == 0 {
		return
	}
	err = logMessages(validUpdateMsgs, s.log)
	if err != nil {
		s.state = oldState
	}

	// forward result to all the update result channels
	// all updates with encounter the same error/success while logging
	for _, update := range validUpdates {
		update.resultCh <- err
	}
}

type sagaUpdate struct {
	msgs     []SagaMessage
	resultCh chan error
}

// logMessages durably stores the messages in sagalog
// if the message(s) is invalid, returns an error and an unmodified saga state
func logMessages(msgs []SagaMessage, log SagaLog) error {
	var err error
	if len(msgs) == 1 {
		err = log.LogMessage(msgs[0])
	} else {
		err = log.LogBatchMessages(msgs)
	}
	return err
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
