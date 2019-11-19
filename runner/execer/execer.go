package execer

import (
	"io"

	"github.com/twitter/scoot/common/errors"
	"github.com/twitter/scoot/common/log/tags"
)

// Execer lets you run one Unix command. It differs from Runner in that it does not
// know about Snapshots or Scoot. It's just a way to run a Unix process (or fake it).
// It's at the level of os/exec, not exec-as-a-service.

// Memory in bytes.
type Memory uint64

type Command struct {
	Argv    []string
	EnvVars map[string]string
	Dir     string
	Stdout  io.Writer
	Stderr  io.Writer
	MemCh   chan ProcessStatus
	tags.LogTags
}

type ProcessState int

const (
	UNKNOWN ProcessState = iota
	RUNNING
	COMPLETE
	FAILED
)

func (s ProcessState) String() string {
	if s == UNKNOWN {
		return "UNKNOWN"
	}
	if s == RUNNING {
		return "RUNNING"
	}
	if s == COMPLETE {
		return "COMPLETE"
	}
	return "FAILED"
}

func (s ProcessState) IsDone() bool {
	return s != RUNNING
}

type Execer interface {
	// Starts process to exec command in a new goroutine.
	Exec(command Command) (Process, error)
}

// TODO why not include directly in Execer?
type Process interface {
	// Blocks until the process terminates.
	Wait() ProcessStatus

	// Terminates process and does best effort to get ExitCode.
	Abort() ProcessStatus
}

// TODO when are these valid in what cases?
type ProcessStatus struct {
	State    ProcessState
	ExitCode errors.ExitCode
	Error    string
}
