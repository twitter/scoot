package saga

import (
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
)

// Randomly generates an Id that is valid for
// use as a sagaId or taskId
func genId(genParams *gopter.GenParameters) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	length := int(genParams.NextUint64()%20) + 1
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[genParams.Rng.Intn(len(chars))]
	}

	return string(result)
}

// Randomly generates a valid SagaState
func genSagaState(genParams *gopter.GenParameters) *SagaState {
	sagaId := genId(genParams)
	data, _ := gen.SliceOf(gen.UInt8()).Sample()
	job := data.([]byte)

	state, err := makeSagaState(sagaId, job)

	if err != nil {
		fmt.Println(err)
	}

	// is saga aborted or not
	isAborted := genParams.NextBool()
	state.sagaAborted = isAborted

	//number of tasks to run in this saga
	numTasks := int(genParams.NextUint64() % 100)

	for i := 0; i < numTasks; i++ {
		taskId := genId(genParams)
		flags := TaskStarted

		// randomly decide if task has been completed
		if genParams.NextBool() {
			flags = flags | TaskCompleted
		}

		if isAborted {
			// randomly decide if comp tasks have started/completed
			if genParams.NextBool() {
				flags = flags | CompTaskStarted
				if genParams.NextBool() {
					flags = flags | CompTaskCompleted
				}
			}
		}

		state.taskState[taskId] = flags
	}

	// check if saga is in completed state then coin flip to decide if we actually log
	// the end complete message
	isCompleted := true
	for _, id := range state.GetTaskIds() {
		if state.IsSagaAborted() {
			if !(state.IsTaskStarted(id) && state.IsCompTaskStarted(id) && state.IsCompTaskCompleted(id)) {
				isCompleted = false
				break
			}
		} else {
			if !(state.IsTaskStarted(id) && state.IsTaskCompleted(id)) {
				isCompleted = false
				break
			}
		}
	}

	if isCompleted && genParams.NextBool() {
		state.sagaCompleted = true
	}

	return state
}

// Generator for a valid SagaId or TaskId
func GenId() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		id := genId(genParams)
		genResult := gopter.NewGenResult(id, gopter.NoShrinker)
		return genResult
	}
}

// Generator for a Valid Saga State
func GenSagaState() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		state := genSagaState(genParams)
		genResult := gopter.NewGenResult(state, gopter.NoShrinker)
		return genResult
	}
}

// State is a struct object and not a reference for readability
// when a counter example is produced
type StateTaskPair struct {
	state  SagaState
	taskId string
}

// Generator for a SagaState and TaskId, returns a StateTaskPair
// SagaState is always valid.  TaskId may or may not be part of the saga
func GenSagaStateAndTaskId() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		state := genSagaState(genParams)

		id := genId(genParams)
		if genParams.NextBool() {
			ids := state.GetTaskIds()
			switch len(ids) {
			case 0:
				//do nothing just use randomly generated id
			case 1:
				id = ids[0]
			default:
				index := genParams.NextUint64() % uint64(len(ids))
				id = ids[index]
			}
		}

		result := StateTaskPair{
			state:  *state,
			taskId: id,
		}

		genResult := gopter.NewGenResult(result, gopter.NoShrinker)
		return genResult
	}
}
