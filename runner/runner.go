package runner

import (
	"fmt"
	"time"
)

type RunId string
type ProcessState int

const (
	// An unambiguous 0-value.
	UNKNOWN ProcessState = iota
	// Waiting to run.
	PENDING
	// Running
	RUNNING

	// States below are end states
	// a Process in an end state will not change its state

	// Ran to completion
	COMPLETED
	// Could not run to completion
	FAILED
)

func (p ProcessState) String() string {
	switch p {
	case UNKNOWN:
		return "UNKNOWN"
	case PENDING:
		return "PENDING"
	case RUNNING:
		return "RUNNING"
	case COMPLETED:
		return "COMPLETED"
	case FAILED:
		return "FAILED"
	default:
		panic(fmt.Sprintf("Unexpected ProcessState %v", int(p)))
	}
}

// A command, execution environment, and timeout.
type Command struct {
	// Command line to run
	Argv []string

	// Key-value pairs for environment variables
	EnvVars map[string]string

	// Timeout
	Timeout time.Duration
}

func NewCommand(argv []string, env map[string]string, timeout time.Duration) *Command {
	return &Command{Argv: argv, EnvVars: env, Timeout: timeout}
}

// Returned by the coordinator when a run request is made.
type ProcessStatus struct {
	RunId RunId
	State ProcessState
	// References to stdout and stderr, not their text
	StdoutRef string
	StderrRef string

	// Only valid if State == COMPLETED
	ExitCode int

	// Only valid if State == FAILED
	Error string
}

type Runner interface {
	// Run instructs the Runner to run cmd. It returns its status and any errors.
	// Run may:
	// enqueue cmd (leading to state PENDING)
	// run cmd immediately (leading to state RUNNING)
	// check if cmd is well-formed, and reject it if not (leading to state FAILED)
	// wait a very short period of time for cmd to finish
	// Run may not wait indefinitely for cmd to finish. This is an async API.
	Run(cmd *Command) (ProcessStatus, error)

	// Status checks the status of run.
	Status(run RunId) (ProcessStatus, error)
}
