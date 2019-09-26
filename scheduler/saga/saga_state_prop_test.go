// +build property_test

package saga

import (
	"bytes"
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	log "github.com/sirupsen/logrus"
	"strings"
	"testing"
)

func Test_ValidateUpdateSagaState(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 1000
	properties := gopter.NewProperties(parameters)

	// property tests were written to expect that updateSaga returned two args,
	// the first a new SagaState iff the transition is valid
	// maintain this API to make the tests easier to write
	updateSagaStateCompat := func(state *SagaState, msg SagaMessage) (*SagaState, error) {
		newState := copySagaState(state)
		err := updateSagaState(newState, msg)
		if err != nil {
			newState = nil
		}
		return newState, err
	}

	properties.Property("StartSaga message is never valid on an already started saga", prop.ForAll(
		func(state *SagaState, data []byte) bool {
			msg := MakeStartSagaMessage(state.SagaId(), data)
			newState, err := updateSagaStateCompat(state, msg)

			return err != nil && newState == nil
		},
		GenSagaState(true),
		gen.SliceOf(gen.UInt8()),
	))

	// EndSaga messages are valid if a saga has not been Aborted and all StartTask have EndTask messages
	// If a saga has been aborted all StartTask messages must have corresponding StartCompTask / EndCompTask messages
	// for an EndSaga message to be valid.
	properties.Property("EndSaga message is Valid or returns an error", prop.ForAll(
		func(state *SagaState) bool {

			msg := MakeEndSagaMessage(state.SagaId())
			newState, err := updateSagaStateCompat(state, msg)

			validTransition := true
			for _, id := range state.GetTaskIds() {

				// if aborted all comp tasks must be completed for all tasks started
				// if not aborted all started tasks must be completed
				if state.IsSagaAborted() {
					if !(state.IsTaskStarted(id) &&
						state.IsCompTaskStarted(id) &&
						state.IsCompTaskCompleted(id)) {
						validTransition = false
					}
				} else {
					if !(state.IsTaskStarted(id) &&
						state.IsTaskCompleted(id)) {
						validTransition = false
					}
				}
			}

			// validate the correct error is returned
			_, sErrorOk := err.(InvalidSagaStateError)

			// either we made a valid transition and had a valid update or applying
			// this message is an invalidTransition and an error was returned.
			validUpdate := validTransition && err == nil &&
				newState != nil && newState.IsSagaCompleted()
			errorReturned := !validTransition && err != nil && sErrorOk && newState == nil

			return validUpdate || errorReturned
		},
		GenSagaState(true),
	))

	// Abort messages are valid unless a Saga has been Completed
	properties.Property("AbortSaga message is valid or returns an error", prop.ForAll(
		func(state *SagaState) bool {
			validTransition := !state.IsSagaCompleted()

			msg := MakeAbortSagaMessage(state.SagaId())
			newState, err := updateSagaStateCompat(state, msg)

			// validate the correct error is returned
			_, sErrorOk := err.(InvalidSagaStateError)

			// either we made a valid transition and had a valid update or applying
			// this message is an invalidTransition and an error was returned.
			validUpdate := validTransition && err == nil &&
				newState != nil && newState.IsSagaAborted()
			errorReturned := !validTransition && err != nil && sErrorOk && newState == nil

			return validUpdate || errorReturned
		},
		GenSagaState(true),
	))

	// StartTask messages are valid unless a Saga has been Completed or Aborted
	properties.Property("StartTask message is valid or returns an Error", prop.ForAll(

		func(pair StateTaskPair, data []byte) bool {

			state := pair.state
			taskId := pair.taskId

			validTransition := !state.IsSagaCompleted() && !state.IsSagaAborted() && !state.IsTaskCompleted(taskId)

			msg := MakeStartTaskMessage(state.SagaId(), taskId, data)
			newState, err := updateSagaStateCompat(state, msg)

			// validate the correct error is returned
			_, sErrorOk := err.(InvalidSagaStateError)

			// either we made a valid transition and had a valid update or applying
			// this message is an invalidTransition and an error was returned.
			validUpdate := validTransition && err == nil &&
				newState != nil && newState.IsTaskStarted(taskId) &&
				bytes.Equal(newState.GetStartTaskData(taskId), data)
			errorReturned := !validTransition && err != nil && sErrorOk && newState == nil

			return validUpdate || errorReturned
		},
		GenSagaStateAndTaskId(),
		gen.SliceOf(gen.UInt8()),
	))

	// EndTask messages are valid if there is a corresponding StartTask message and a Saga
	// has not been aborted or completed
	properties.Property("EndTask message is valid or returns an Error", prop.ForAll(
		func(pair StateTaskPair, data []byte) bool {

			state := pair.state
			taskId := pair.taskId

			validTransition := !state.IsSagaCompleted() && !state.IsSagaAborted() &&
				state.IsTaskStarted(taskId)

			msg := MakeEndTaskMessage(state.SagaId(), taskId, data)
			newState, err := updateSagaStateCompat(state, msg)

			// validate the correct error is returned
			_, sErrorOk := err.(InvalidSagaStateError)

			// either we made a valid transition and had a valid update or applying
			// this message is an invalidTransition and an error was returned.
			validUpdate := validTransition && err == nil &&
				newState != nil && newState.IsTaskCompleted(taskId) &&
				bytes.Equal(newState.GetEndTaskData(taskId), data)
			errorReturned := !validTransition && err != nil && sErrorOk && newState == nil

			return validUpdate || errorReturned
		},
		GenSagaStateAndTaskId(),
		gen.SliceOf(gen.UInt8()),
	))

	properties.Property("StartCompTask message is valid or returns an Error", prop.ForAll(
		func(pair StateTaskPair, data []byte) bool {

			state := pair.state
			taskId := pair.taskId

			validTransition := state.IsSagaAborted() && !state.IsSagaCompleted() &&
				state.IsTaskStarted(taskId) && !state.IsCompTaskCompleted(taskId)

			msg := MakeStartCompTaskMessage(state.SagaId(), taskId, data)
			newState, err := updateSagaStateCompat(state, msg)

			// validate the correct error is returned
			_, sErrorOk := err.(InvalidSagaStateError)

			// either we made a valid transition and had a valid update or applying
			// this message is an invalidTransition and an error was returned.
			validUpdate := validTransition && err == nil &&
				newState != nil && newState.IsCompTaskStarted(taskId) &&
				bytes.Equal(newState.GetStartCompTaskData(taskId), data)
			errorReturned := !validTransition && err != nil && sErrorOk && newState == nil

			return validUpdate || errorReturned
		},
		GenSagaStateAndTaskId(),
		gen.SliceOf(gen.UInt8()),
	))

	properties.Property("EndCompTask message is valid or returns an Error", prop.ForAll(
		func(pair StateTaskPair, data []byte) bool {

			state := pair.state
			taskId := pair.taskId

			validTransition := state.IsSagaAborted() && !state.IsSagaCompleted() &&
				state.IsTaskStarted(taskId) && state.IsCompTaskStarted(taskId)

			msg := MakeEndCompTaskMessage(state.SagaId(), taskId, data)
			newState, err := updateSagaStateCompat(state, msg)

			// validate the correct error is returned
			_, sErrorOk := err.(InvalidSagaStateError)

			// either we made a valid transition and had a valid update or applying
			// this message is an invalidTransition and an error was returned.
			validUpdate := validTransition && err == nil &&
				newState != nil && newState.IsCompTaskCompleted(taskId) &&
				bytes.Equal(newState.GetEndCompTaskData(taskId), data)
			errorReturned := !validTransition && err != nil && sErrorOk && newState == nil

			return validUpdate || errorReturned
		},
		GenSagaStateAndTaskId(),
		gen.SliceOf(gen.UInt8()),
	))

	properties.Property("String method returns correct representation of SagaState", prop.ForAll(
		func(state *SagaState) bool {

			str := state.String()

			if !strings.Contains(str, fmt.Sprintf("SagaId: %v", state.SagaId())) {
				return false
			}

			if state.IsSagaAborted() && !strings.Contains(str, "SagaAborted: true") {
				return false
			}

			if !state.IsSagaAborted() && !strings.Contains(str, "SagaAborted: false") {
				return false
			}

			if state.IsSagaCompleted() && !strings.Contains(str, "SagaCompleted: true") {
				return false
			}

			if !state.IsSagaCompleted() && !strings.Contains(str, "SagaCompleted: false") {
				return false
			}

			if len(state.GetTaskIds()) > 0 {
				taskSplit := strings.Split(str, "Tasks: [")
				taskString := strings.Split(taskSplit[1], ",")

				for _, taskStr := range taskString {

					split := strings.Split(taskStr, ": ")

					if len(split) >= 2 {
						taskId := strings.TrimSpace(split[0])
						taskStates := strings.Split(split[1], "|")

						for _, taskState := range taskStates {

							ts := strings.TrimSpace(taskState)
							switch ts {
							case "Started":
								if !state.IsTaskStarted(taskId) {
									return false
								}
							case "Completed":
								if !state.IsTaskCompleted(taskId) {
									return false
								}
							case "CompTaskStarted":
								if !state.IsCompTaskStarted(taskId) {
									return false
								}
							case "CompTaskCompleted":
								if !state.IsCompTaskCompleted(taskId) {
									return false
								}
							default:
								log.Info(fmt.Sprintf("Unrecognized state taskId: %s, taskState: %s ", taskId, taskState))
								return false
							}
						}
					}
				}
			}

			return true
		},
		GenSagaState(true),
	))

	properties.TestingRun(t)
}
