package saga

import (
	//"fmt"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"testing"
)

func genSagaState() gopter.Gen {

	return func(genParams *gopter.GenParameters) *gopter.GenResult {

		sId, _ := gen.AnyString().Sample()
		sagaId := sId.(string)

		data, _ := gen.SliceOf(gen.UInt8()).Sample()
		job := data.([]byte)

		state, _ := makeSagaState(sagaId, job)

		// is saga aborted or not
		isAborted := genParams.NextBool()
		state.sagaAborted = isAborted

		//number of tasks to run in this saga
		numTasks := int(genParams.NextUint64() % 100)

		for i := 0; i < numTasks; i++ {
			tId, _ := gen.AnyString().Sample()
			taskId := tId.(string)
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

		//fmt.Println("Generated Saga with %s tasks", numTasks)

		genResult := gopter.NewGenResult(state, gopter.NoShrinker)
		return genResult
	}
}

func Test_ValidSagaId(t *testing.T) {
	properties := gopter.NewProperties(nil)
	properties.Property("Empty String is Invalid SagaId", prop.ForAll(
		func(id string) bool {
			err := validateSagaId(id)

			return (err != nil && id == "") || err == nil
		},
		gen.AnyString(),
	))

	properties.TestingRun(t)
}

func Test_ValidTaskId(t *testing.T) {
	properties := gopter.NewProperties(nil)
	properties.Property("Empty String is Invalid TaskId", prop.ForAll(
		func(id string) bool {
			err := validateTaskId(id)

			return (err != nil && id == "") || err == nil
		},
		gen.AnyString(),
	))

	properties.TestingRun(t)
}

func Test_ValidateUpdateSagaState(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("EndSaga message is Valid or returns an error", prop.ForAll(
		func(state *SagaState) bool {

			msg := MakeEndSagaMessage(state.SagaId())

			newState, err := updateSagaState(state, msg)

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

			return (validTransition && err == nil && newState != nil) ||
				(!validTransition && err != nil && newState == nil)
		},
		genSagaState(),
	))

	properties.TestingRun(t)
}

//todo a successful saga state transition should always be able to be recovered

/*switch err {
  case nil:
    switch msg.msgType {

      // you can always log a Start Saga Message
    case StartSaga:
      return true

      // can only end a saga if all tasks & compensating tasks have been completed
    case EndSaga:

      for _, id := range state.GetTaskIds {
        if(state.isAborted) {
          if !(state.IsTaskStarted(id) &&
              state.IsCompTaskStarted(id) &&
              state.isCompTaskCompleted(id)) {
            return false
          }
        } else {
          if !(state.IsTaskStarted(id) &&
            state.IsTaskCompleted(id)) {
            return false
          }
        }
      }
      return true

      // you can always log an Abort Saga Message
    case AbortSaga:
      return true

      // you can't start a task if a saga is aborted : TODO Caitie fix in code
    case StartTask:
      return true

      // you can't end a task if the saga is aborted : TODO Caitie fix
      // you can't end a task if the task has not been started
    case EndTask:
      id := msg.taskId

      if !state.IsTaskStarted(id) {
        return false
      }

    case StartCompTask:
    case EndCompTask:
    }
  default:
  }
}*/
