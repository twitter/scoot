package scheduler

import (
	"fmt"

	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/queue"
)

// Retrievews work from the work queue and sends it to the scheduler
// To be scheduled and completed.
func GenerateWork(scheduler Scheduler, workCh chan queue.WorkItem) {
	for workItem := range workCh {
		job := workItem.Job()
		err := scheduler.ScheduleJob(job)

		if err != nil {
			if !saga.FatalErr(err) {
				// TODO: Implement deadletter queue.  SagaLog is failing to store this message some reason,
				// Could be bad message or could be because the log is unavailable.  Put on Deadletter Queue and Move On
				// For now just panic, for Alpha (all in memory this SHOULD never happen)
				panic(fmt.Sprintf("Failed to succeesfully Write to SagaLog this Job should be put on the deadletter queue.  Err: %v", err))
			} else {
				// Something is really wrong. Either StartSaga message is formatted incorrectly when writing to SagaLog
				// or the Job pulled off the work queue is invalid.
				panic(fmt.Sprintf("Fatal Error Starting Job.  Err: %v", err))
			}
		}

		workItem.Dequeue()
	}
}
