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

	// Run completed, possibly with nonzero exit code.
	COMPLETE
	// Could not complete, bad request or golang runtime err.
	FAILED
	// User requested that the run be killed.
	ABORTED
	// Operation timed out and was killed.
	TIMEDOUT
)

func (p ProcessState) IsDone() bool {
	return p == COMPLETE || p == FAILED || p == ABORTED || p == TIMEDOUT
}

func (p ProcessState) String() string {
	switch p {
	case UNKNOWN:
		return "UNKNOWN"
	case PENDING:
		return "PENDING"
	case RUNNING:
		return "RUNNING"
	case COMPLETE:
		return "COMPLETE"
	case FAILED:
		return "FAILED"
	case ABORTED:
		return "ABORTED"
	case TIMEDOUT:
		return "TIMEDOUT"
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

	// Only valid if State == COMPLETE
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
	Run(cmd *Command) ProcessStatus

	// Status checks the status of run.
	Status(run RunId) ProcessStatus

	// Current status of all runs, running and finished.
	StatusAll() []ProcessStatus

	// Kill the given run.
	Abort(run RunId) ProcessStatus

	// Prunes the run history so StatusAll() can return a reasonable number of runs.
	Erase(run RunId)
}
