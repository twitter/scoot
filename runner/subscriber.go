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

func Current() PollOpts {
	return PollOpts{Timeout: time.Duration(0)}
}

func Wait() PollOpts {
	return PollOpts{Timeout: time.Duration(-1)}
}
