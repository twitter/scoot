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
		updateChan: make(chan stateUpdate, 0),
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

				// Put StartTask Message on SagaLog
				jr.logSagaMessage(saga.StartTask, task.Id, nil)

				jr.wg.Add(1)
				go func(node cm.Node, task sched.Task) {
					defer jr.dist.ReleaseNode(node)
					defer jr.wg.Done()

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

					jr.logSagaMessage(saga.EndTask, task.Id, nil)
				}(node, task)
			}
		}
	} else {
		// TODO: we don't have a way to specify comp tasks yet
		// Once we do they should be ran here.  Currently the
		// scheduler only supports ForwardRecovery in Sagas so Panic!
		panic("Rollback Recovery Not Supported Yet!")
	}

	// wait for all tasks to complete
	jr.wg.Wait()

	// Log EndSaga Message to SagaLog
	jr.logSagaMessage(saga.EndSaga, "", nil)

	return
}

type stateUpdate struct {
	msgType  saga.SagaMessageType
	taskId   string
	data     []byte
	loggedCh chan bool
}

// Logs the message durably to the sagalog and updates the state.  Blocks until the message
// has been durably logged
func (jr jobRunner) logSagaMessage(msgType saga.SagaMessageType, taskId string, data []byte) {
	update := stateUpdate{
		msgType:  msgType,
		taskId:   taskId,
		data:     data,
		loggedCh: make(chan bool, 0),
	}

	jr.updateChan <- update

	// wait for update to complete before returning
	<-update.loggedCh
	return
}

// handles all the sagaState updates
func (jr jobRunner) updateSagaState() {

	var err error
	var state *saga.SagaState

	for update := range jr.updateChan {
		switch update.msgType {
		case saga.StartTask:
			state, err = jr.saga.StartTask(jr.sagaState, update.taskId, update.data)

		case saga.EndTask:
			state, err = jr.saga.EndTask(jr.sagaState, update.taskId, update.data)

		case saga.StartCompTask:
			state, err = jr.saga.StartCompensatingTask(jr.sagaState, update.taskId, update.data)

		case saga.EndCompTask:
			state, err = jr.saga.EndCompensatingTask(jr.sagaState, update.taskId, update.data)

		case saga.EndSaga:
			state, err = jr.saga.EndSaga(jr.sagaState)

		case saga.AbortSaga:
			state, err = jr.saga.AbortSaga(jr.sagaState)
		}

		// if state transition was successful update internal state
		if err == nil {
			jr.sagaState = state
			update.loggedCh <- true

		} else {
			if retryableErr(err) {
				// TODO: Implement deadletter queue.  SagaLog is failing to store this message some reason,
				// Could be bad message or could be because the log is unavailable.  Put on Deadletter Queue and Move On
				// For now just panic, for Alpha (all in memory this SHOULD never happen)
				panic(fmt.Sprintf("Failed to succeesfully log StartCompTask sagaId: %v, taskId: %v, this Job should be put on the deadletter queue", jr.sagaState.SagaId(), update.taskId))
			} else {
				// Something is really wrong.  Either an Invalid State Transition, or we formatted the request to the SagaLog incorrectly
				// These errors indicate a fatal bug in our code.  So we should panic.
				panic(fmt.Sprintf("Failed to succeesfully log StartCompTask. sagaId: %v, taskId: %v", jr.sagaState.SagaId(), update.taskId))
			}
		}
	}
}

// checks the error returned my updating saga state.
func retryableErr(err error) bool {

	switch err.(type) {
	// InvalidSagaState is an unrecoverable error. This indicates a fatal bug in the code
	// which is asking for an impossible transition.
	case saga.InvalidSagaStateError:
		return false

	// InvalidSagaMessage is an unrecoverable error.  This indicates a fatal bug in the code
	// which is applying invalid parameters to a saga.
	case saga.InvalidSagaMessageError:
		return false

	// InvalidRequestError is an unrecoverable error.  This indicates a fatal bug in the code
	// where the SagaLog cannot durably store messages
	case saga.InvalidRequestError:
		return false

	// InternalLogError is a transient error experienced by the log.  It was not
	// able to durably store the message but it is ok to retry.
	case saga.InternalLogError:
		return true

	// unknown error, default to retryable.
	default:
		return true
	}
}
