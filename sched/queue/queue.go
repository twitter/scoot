package queue

import (
	"github.com/scootdev/scoot/sched"
	"time"
)

// Note: queued work items can be read straight out of a channel that should
// be returned along with the creator of a Queue
type Queue interface {
	// Enqueue enqueues a job and assigns it an ID. Errors may be
	// an *InvalidJobRequest or a *CanNotScheduleNow
	Enqueue(job sched.JobDefinition) (string, error)
	Close() error
}

// WorkItem is work from the work queue.
type WorkItem interface {
	// Job gets the job definition for this work item.
	Job() sched.Job

	// Dequeue dequeues the work item from the queue. Callers should only call this
	// once the Job has been persisted to the next storage system.
	Dequeue()
}

func NewInvalidJobRequest(errMsg string) error {
	return &InvalidJobRequest{errMsg}
}

type InvalidJobRequest struct {
	errMsg string
}

func (e *InvalidJobRequest) Error() string {
	return e.errMsg
}

func NewCanNotScheduleNow(untilRetry time.Duration, errMsg string) error {
	return &CanNotScheduleNow{untilRetry, errMsg}
}

type CanNotScheduleNow struct {
	UntilRetry time.Duration
	errMsg     string
}

func (e *CanNotScheduleNow) Error() string {
	return e.errMsg
}

// Validate a job, returning an *InvalidJobRequest if invalid.
func ValidateJob(job sched.JobDefinition) error {
	if len(job.Tasks) == 0 {
		return NewInvalidJobRequest("invalid job. Must have at least 1 task; was empty")
	}
	for id, task := range job.Tasks {
		if id == "" {
			return NewInvalidJobRequest("invalid task id \"\".")
		}
		if len(task.Command.Argv) == 0 {
			return NewInvalidJobRequest("invalid task.Command.Argv. Must have at least one argument; was empty")
		}
	}
	return nil
}
