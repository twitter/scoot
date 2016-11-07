package runner

import (
	"time"
)

// PollOpts describes options for our Poll
type PollOpts struct {
	// How long to wait for new Events
	Timeout time.Duration

	// We might add:
	// maximum number of evens to return
	// MaxEvents int
}

const NoPoll = PollOpts{Timeout: time.Duration(0)}

type StatusQuery struct {
	Runs   []RunId
	States StateMask
}

type Statuses interface {
	Query(q StatusQuery, poll PollOpts) ([]ProcessStatus, error)
}
