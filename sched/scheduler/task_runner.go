package scheduler

import (
	"log"

	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/runner"
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
// statusAddr: the http host:port for worker. Empty values are ignored.
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

	// Log the StartTask message with an empty payload. This will be overwritten with more data when we have it.
	err := saga.StartTask(taskId, nil)
	if err != nil {
		return err
	}

	// Handle errors the same way for Start()/Wait().
	// Overwrites selected processStatus fields and leaves other values intact.
	// If err, check markCompletedOnFailure. If true, log and continue. If false, return err immediately.
	processStatus := runner.ProcessStatus{}
	workerErrStatus := func(err error) error {
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
		return nil
	}

	// Start task on the worker, then re-log Start Task with data this time, then block on task completion.
	// Clients can watch progress via the ProcessStatus Stdout/Stderr URIs but won't see any other updates until completion.
	// Note: Stdout/Stderr URIs should be present here given a SimpleRunner-backed worker (or any new conforming runner impls).
	//       URIs will likely change upon task completion, i.e. from local file to snapshot URI.
	var startErr error
	processStatus, startErr = worker.Start(task)
	if workerErrStatus(startErr) != nil {
		return startErr
	}

	statusAsBytes, err := workerapi.SerializeProcessStatus(processStatus)
	if err != nil {
		stat.Counter("failedTaskSerializeCounter").Inc(1)
		return err
	}
	err = saga.StartTask(taskId, statusAsBytes)
	if err != nil {
		return err
	}

	// Wait for worker to finish the task.
	// If Start() has already failed, we skip this and proceed to the saga log section.
	if startErr == nil {
		processStatus, err = worker.Wait(processStatus.RunId)
		if workerErrStatus(err) != nil {
			return err
		}
	}

	statusAsBytes, err = workerapi.SerializeProcessStatus(processStatus)
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
