package scheduler

import (
	"fmt"
	s "github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	"github.com/scootdev/scoot/sched/distributor"
	"sync"
)

// Run the Job associated with this Saga to completion.  If its a brand new job
// all tasks will be ran.  If its an in-progress Saga, only uncompleted tasks
// will be executed
func runJob(job sched.Job, saga *s.Saga, nodes []cm.Node) {

	var wg sync.WaitGroup

	// don't re-run an already completed saga
	initialSagaState := saga.GetState()
	if initialSagaState.IsSagaCompleted() {
		return
	}

	dist := distributor.NewPoolDistributor(cm.StaticClusterFactory(nodes))

	// if the saga has not been aborted, just run each task in the saga
	// TODO: this can be made smarter to run the next Best Task instead of just
	// the next one in order etc....
	if !initialSagaState.IsSagaAborted() {
		for _, task := range job.Tasks {

			if !initialSagaState.IsTaskCompleted(task.Id) {
				node := dist.ReserveNode()

				// Put StartTask Message on SagaLog
				err := saga.StartTask(task.Id, nil)
				if err != nil {
					handleSagaLogErrors(err)
				}

				wg.Add(1)
				go func(node cm.Node, task sched.Task) {
					defer dist.ReleaseNode(node)
					defer wg.Done()

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

					err := saga.EndTask(task.Id, nil)
					if err != nil {
						handleSagaLogErrors(err)
					}

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
	wg.Wait()

	// Log EndSaga Message to SagaLog
	err := saga.EndSaga()
	if err != nil {
		handleSagaLogErrors(err)
	}

	return
}

func handleSagaLogErrors(err error) {
	if !s.FatalErr(err) {
		// TODO: Implement deadletter queue.  SagaLog is failing to store this message some reason,
		// Could be bad message or could be because the log is unavailable.  Put on Deadletter Queue and Move On
		// For now just panic, for Alpha (all in memory this SHOULD never happen)
		panic(fmt.Sprintf("Failed to succeesfully Write to SagaLog this Job should be put on the deadletter queue.  Err: %v", err))
	} else {
		// Something is really wrong.  Either an Invalid State Transition, or we formatted the request to the SagaLog incorrectly
		// These errors indicate a fatal bug in our code.  So we should panic.
		panic(fmt.Sprintf("Fatal Error Writing to SagaLog.  Err: %v", err))
	}
}
