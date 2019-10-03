package thrift

import (
	"time"
)

// Error to be returned when a Job Request cannot be processed
// because the request is malformed.  Retrying this request
// will never result in a success.
//
// Analogous to an HTTP 400 level error
type InvalidJobRequest struct {
	errMsg string
}

func NewInvalidJobRequest(errMsg string) error {
	return &InvalidJobRequest{errMsg}
}

func (e *InvalidJobRequest) Error() string {
	return e.errMsg
}

// Error to be returned when a Job cannot be scheduled because
// of a transient error. i.e. Scheduler is too busy, SagaLog is
// Unavailable etc...
//
// Error indicates that the user could retry the request at another
// time and it will succeed
//
// Analogous to an HTTP 500 level error
type CanNotScheduleNow struct {
	UntilRetry time.Duration
	errMsg     string
}

func NewCanNotScheduleNow(untilRetry time.Duration, errMsg string) error {
	return &CanNotScheduleNow{untilRetry, errMsg}
}

func (e *CanNotScheduleNow) Error() string {
	return e.errMsg
}
