package queue

import (
	"github.com/scootdev/scoot/sched"
	"time"
)

type Queue interface {
	Enqueue(job sched.Job) (string, error)
	Close() error
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
