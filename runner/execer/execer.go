package execer

import "io"

// Execer lets you run one Unix command. It differs from Runner in that it does not
// know about Snapshots or Scoot. It's just a way to run a Unix process (or fake it).
// It's at the level of os/exec, not exec-as-a-service.

// Memory in bytes.
type Memory uint64

type Command struct {
	Argv []string
	Dir  string

	Stdout io.Writer
	Stderr io.Writer
	// TODO(dbentley): environment variables?
}

type ProcessState int

const (
	UNKNOWN ProcessState = iota
	RUNNING
	COMPLETE
	FAILED
)

func (s ProcessState) IsDone() bool {
	return s == COMPLETE || s == FAILED
}

type Execer interface {
	// Starts process to exec command in a new goroutine.
	Exec(command Command) (Process, error)
}

type Process interface {
	// TODO(dbentley): perhaps have a poll method?

	// Blocks until the process terminates.
	Wait() ProcessStatus

	// Terminates process and does best effort to get ExitCode.
	Abort() ProcessStatus

	// Measure the current amount of resident memory used by this process.
	MemUsage() (Memory, error)
}

type ProcessStatus struct {
	State    ProcessState
	ExitCode int
	Error    string
}
