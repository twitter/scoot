package scheduler

import (
	"fmt"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	dist "github.com/scootdev/scoot/sched/distributor"
	"sync"
)

const MAX_RETRY = 3

type jobRunner struct {
	job       sched.Job
	saga      saga.Saga
	sagaState *saga.SagaState
	dist      *dist.PoolDistributor

	updateChan chan stateUpdate
	wg         sync.WaitGroup
}

func NewJobRunner(job sched.Job, saga saga.Saga, sagaState *saga.SagaState, nodes []cm.Node) jobRunner {
	jr := jobRunner{
		job:        job,
		saga:       saga,
		sagaState:  sagaState,
		dist:       dist.NewPoolDistributor(cm.StaticClusterFactory(nodes)),
		updateChan: make(chan stateUpdate),
	}

	go jr.updateSagaState()
	return jr
}

// Runs the Job associated with this JobRunner to completion
func (jr jobRunner) runJob() {
	// don't re-run an already completed saga
	if jr.sagaState.IsSagaCompleted() {
		return
	}

	// if the saga has not been aborted, just run each task in the saga
	// TODO: this can be made smarter to run the next Best Task instead of just
	// the next one in order etc....
	if !jr.sagaState.IsSagaAborted() {
		for _, task := range jr.job.Tasks {

			if !jr.sagaState.IsTaskCompleted(task.Id) {
				node := jr.dist.ReserveNode()

				jr.updateChan <- stateUpdate{
					msgType: saga.StartTask,
					taskId:  task.Id,
				}

				jr.wg.Add(1)
				go func(node cm.Node, task sched.Task) {

					// TODO: After a number of attempts we should stop
					// Trying to run a task, could be a poison pill
					// Implement deadletter queue
					taskExecuted := false
					for !taskExecuted {
						err := node.SendMessage(task)
						if err == nil {
							taskExecuted = true
						}
					}

					jr.updateChan <- stateUpdate{
						msgType: saga.EndTask,
						taskId:  task.Id,
					}
					jr.dist.ReleaseNode(node)
					jr.wg.Done()
				}(node, task)
			}
		}
	}

	if jr.sagaState.IsSagaAborted() {
		// TODO: we don't have a way to specify comp tasks yet
		// Once we do they should be ran here.  Currently the
		// scheduler only supports ForwardRecovery in Sagas so Panic!
		panic("Rollback Recovery Not Supported Yet!")
	}

	// wait for all tasks to complete
	jr.wg.Wait()
	jr.updateChan <- stateUpdate{
		msgType: saga.EndSaga,
	}

	return
}

type stateUpdate struct {
	msgType saga.SagaMessageType
	taskId  string
	data    []byte
}

// handles all the sagaState updates
// TODO: lots of shared retry code, need either Lambdas(go support?) or some refactor to re-use shared code.
func (jr jobRunner) updateSagaState() {

	for update := range jr.updateChan {
		switch update.msgType {
		case saga.StartTask:

			attempts := 0
			state, err := jr.saga.StartTask(jr.sagaState, update.taskId, update.data)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.StartTask(jr.sagaState, update.taskId, update.data)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state

				// state transition was not successful maxed out retries
				// TODO: for now just panic eventually implement dead letter queue.
			} else {
				panic(fmt.Sprintf("Failed to succeesfully log StartTask. sagaId: %v, taskId: %v", jr.sagaState.SagaId(), update.taskId))
			}

		case saga.EndTask:
			attempts := 0
			state, err := jr.saga.EndTask(jr.sagaState, update.taskId, update.data)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.EndTask(jr.sagaState, update.taskId, update.data)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state

				// state transition was not successful maxed out retries
				// TODO: for now just panic eventually implement dead letter queue.
			} else {
				panic(fmt.Sprintf("Failed to succeesfully log EndTask sagaId: %v, taskId: %v", jr.sagaState.SagaId(), update.taskId))
			}

		case saga.StartCompTask:
			attempts := 0
			state, err := jr.saga.StartCompensatingTask(jr.sagaState, update.taskId, update.data)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.StartCompensatingTask(jr.sagaState, update.taskId, update.data)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state

				// state transition was not successful maxed out retries
				// TODO: for now just panic eventually implement dead letter queue.
			} else {
				panic(fmt.Sprintf("Failed to succeesfully log StartCompTask. sagaId: %v, taskId: %v", jr.sagaState.SagaId(), update.taskId))
			}

		case saga.EndCompTask:
			attempts := 0
			state, err := jr.saga.EndCompensatingTask(jr.sagaState, update.taskId, update.data)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.EndCompensatingTask(jr.sagaState, update.taskId, update.data)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state

				// state transition was not successful maxed out retries
				// TODO: for now just panic eventually implement dead letter queue.
			} else {
				panic(fmt.Sprintf("Failed to succeesfully log EndCompTask. sagaId: %v, taskId: %v", jr.sagaState.SagaId(), update.taskId))
			}

		case saga.EndSaga:
			attempts := 0
			state, err := jr.saga.EndSaga(jr.sagaState)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.EndSaga(jr.sagaState)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state
				// close the channel no more messages should be coming
				close(jr.updateChan)

				// state transition was not successful maxed out retries
				// TODO: for now just panic eventually implement dead letter queue.
			} else {
				panic(fmt.Sprintf("Failed to succeesfully log EndSaga. sagaId: %v", jr.sagaState.SagaId()))
			}

		case saga.AbortSaga:
			attempts := 0
			state, err := jr.saga.AbortSaga(jr.sagaState)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.AbortSaga(jr.sagaState)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state
			} else {
				panic(fmt.Sprintf("Failed to successfully log AbortSaga. sagaId: %v", jr.sagaState.SagaId()))
			}
		}
	}
}

// checks the error returned my updating saga state.
func retryableErr(err error) bool {

	// InvalidSagaState is an unrecoverable error. This indicates a fatal bug in the code
	// which is asking for an impossible transition.
	_, ok := err.(saga.InvalidSagaStateError)
	if ok {
		return false
	}

	// InvalidSagaMessage is an unrecoverable error.  This indicates a fatal bug in the code
	// which is applying invalid parameters to a saga.
	_, ok2 := err.(saga.InvalidSagaMessageError)
	if ok2 {
		return false
	}

	// InvalidRequestError is an unrecoverable error.  This indicates a fatal bug in the code
	// where the SagaLog cannot durably store messages
	_, ok3 := err.(saga.InvalidRequestError)
	if ok3 {
		return false
	}

	// InternalLogError is a transient error experienced by the log.  It was not
	// able to durably store the message but it is ok to retry.
	_, ok4 := err.(saga.InternalLogError)
	if ok4 {
		return true
	}

	// unknown error, default to retryable.
	return true
}
