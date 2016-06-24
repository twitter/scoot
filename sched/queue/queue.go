package queue

import (
	"github.com/scootdev/scoot/sched"
	"time"
)

type Queue interface {
	Enqueue(job sched.Job) (sched.JobID, error)
	Read() (chan sched.Job, error)
	Close() error
}

type InvalidJobRequest struct {
}

type CanNotScheduleNow struct {
	UntilRetry time.Duration
}
