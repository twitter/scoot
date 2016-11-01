package scheduler

import (
	"log"

	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/workerapi"
)

const DeadLetterExitCode = -200

// Run the task on the specified worker, and update the SagaLog appropriately.  Returns an error if one
// occurs while running a task or writing to the SagaLog.  This method blocks until all saga messages
// are logged and the task completes
// parameters:
// saga : the Saga tracking this task
// worker : the worker node to run this task on
// taskId : the Id of this task
// task : the definition of this task
// markCompleteOnFailure : boolean specifying whether a failed test case constitutes completion
// stat : stats to log to
func runTaskAndLog(
	saga *saga.Saga,
	worker worker.Worker,
	taskId string,
	task sched.TaskDefinition,
	markCompleteOnFailure bool,
	stat stats.StatsReceiver) error {
	// Log StartTask Message to SagaLog
	err := saga.StartTask(taskId, nil)
	if err != nil {
		return err
	}
	// runtask on worker
	processStatus, err := worker.RunAndWait(task)
	if err != nil {
		stat.Counter("failedTaskRunCounter").Inc(1)

		// if we should mark tasks complete even if an error occurs set processStatus
		// and don't return.  Otherwise return the error and the task will
		// get rescheduled.
		if markCompleteOnFailure {
			processStatus.Error = err.Error()
			processStatus.ExitCode = DeadLetterExitCode
			log.Printf(
				`Error Running Task %v: dead lettering task after max retries.  
				TaskDef: %+v, Saga Id: %v, Error: %v`,
				taskId, task, saga.GetState().SagaId(), err)
		} else {
			//TODO: Check error to see if its retryable.
			return err
		}
	}

	statusAsBytes, err := workerapi.SerializeProcessStatus(processStatus)
	if err != nil {
		stat.Counter("failedTaskSerializeCounter").Inc(1)
		return err
	}

	// Log EndTask Message to SagaLog
	err = saga.EndTask(taskId, statusAsBytes)
	if err != nil {
		stat.Counter("failedTaskSagaCounter").Inc(1)
	} else {
		stat.Counter("completedTaskCounter").Inc(1)
	}

	return err
}
