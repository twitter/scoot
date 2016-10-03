package runner

import (
	"bytes"
	"fmt"
	"time"
)

// A command, execution environment, and timeout.
type Command struct {
	// Command line to run
	Argv []string

	// Key-value pairs for environment variables
	EnvVars map[string]string

	// Kill command after timeout. Zero value is ignored.
	Timeout time.Duration

	// Runner can optionally use this to run against a particular snapshot. Empty value is ignored.
	//TODO: plumb this through.
	SnapshotId string
}

func (c Command) String() string {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("Command\n\tSnapshot ID:\t%s\n\tArgv:\t%q\n\tTimeout:\t%v\n",
		c.SnapshotId,
		c.Argv,
		c.Timeout))

	if len(c.EnvVars) > 0 {
		b.WriteString("\tEnv:\n")
		for k, v := range c.EnvVars {
			b.WriteString(fmt.Sprintf("\t\t%s: %s\n", k, v))
		}
	}

	return b.String()
}

func NewCommand(argv []string, env map[string]string, timeout time.Duration, snapshotId string) *Command {
	return &Command{Argv: argv, EnvVars: env, Timeout: timeout, SnapshotId: snapshotId}
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

	// Current status of all runs, running and finished, excepting any Erase()'s runs.
	StatusAll() ([]ProcessStatus, error)

	// Kill the given run.
	Abort(run RunId) (ProcessStatus, error)

	// Prunes the run history so StatusAll() can return a reasonable number of runs.
	Erase(run RunId) error
}
