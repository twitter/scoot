package saga

import (
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	log "github.com/sirupsen/logrus"
	"github.com/twitter/scoot/common/thrifthelpers"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
	"math"
)

//
// Saga Generators contains Generator methods that are useful
// when doing property based testing
//

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

// Randomly generates a valid SagaState, with a random job def if includeJob
func genSagaState(genParams *gopter.GenParameters, includeJob bool) *SagaState {
	sagaId := genId(genParams)
	data, _ := gen.SliceOf(gen.UInt8()).Sample()
	if !includeJob {
		data = []uint8{}
	}
	job := data.([]byte)

	state, err := makeSagaState(sagaId, job)

	if err != nil {
		log.Info(err)
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
			err := genTaskCompletedData(state, taskId, genParams)
			if err != nil {
				log.Infof(fmt.Sprintf("Error generating complete task data for %s, %s", taskId, err.Error()))
			}
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

func genTaskCompletedData(state *SagaState, taskID string, genParams *gopter.GenParameters) error {

	runStatus := scoot.RunStatus{}
	runStatus.RunId = taskID
	var statusVal scoot.RunStatusState
	// status must be complete or higher
	statusVal = scoot.RunStatusState(int64(math.Trunc(math.Abs(float64(genParams.NextInt64()%5)))) + 3)
	// don't let the status be nil
	if &statusVal == nil {
		statusVal = scoot.RunStatusState_COMPLETE
	} else {
		statusVal = scoot.RunStatusState(statusVal)
	}
	runStatus.Status = statusVal

	exitCodeStatus := int32(genParams.NextInt64() % 4)
	var ok int32 = 0
	var notOk int32 = -1
	if exitCodeStatus == 0 {
		runStatus.ExitCode = &ok
	} else {
		runStatus.ExitCode = &notOk
	}
	t1 := fmt.Sprintf("error %d", genParams.NextInt64())
	runStatus.Error = &t1
	t2 := fmt.Sprintf("error URI %d", genParams.NextInt64())
	runStatus.ErrUri = &t2
	t3 := fmt.Sprintf("out URI %d", genParams.NextInt64())
	runStatus.OutUri = &t3

	messageAsBytes, err := thrifthelpers.JsonSerialize(&runStatus)

	if err != nil {
		return err
	}

	state.addTaskData(taskID, EndTask, messageAsBytes)

	return nil
}

// Generator for a valid SagaId or TaskId
func GenId() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		id := genId(genParams)
		genResult := gopter.NewGenResult(id, gopter.NoShrinker)
		return genResult
	}
}

// Generator for a Valid Saga State, with a job def if includeJob
func GenSagaState(includeJob bool) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		state := genSagaState(genParams, includeJob)
		genResult := gopter.NewGenResult(state, gopter.NoShrinker)
		return genResult
	}
}

type StateTaskPair struct {
	state  *SagaState
	taskId string
}

func (p StateTaskPair) String() string {
	return fmt.Sprintf("{ TaskId: %v, SagaState: %s }", p.taskId, p.state)
}

// Generator for a SagaState and TaskId, returns a StateTaskPair
// SagaState is always valid.  TaskId may or may not be part of the saga
func GenSagaStateAndTaskId() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		state := genSagaState(genParams, true)

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
			state:  state,
			taskId: id,
		}

		genResult := gopter.NewGenResult(result, gopter.NoShrinker)
		return genResult
	}
}
