// Package runner provides for execution of Scoot work and retrieval
// of the results of that work. The core of this is the Runner interface.
// This package includes a standard runner as well as test and simulation
// implementations.
package runner

import (
	"bytes"
	"fmt"
	"time"
)

const NoRunnersMsg = "No runners available."
const RunnerBusyMsg = "Runner is busy"
const LoggingErrMsg = "Error initializing logging."

// A command, execution environment, and timeout.
type Command struct {
	// Command line to run
	Argv []string

	// Key-value pairs for environment variables
	EnvVars map[string]string

	// Kill command after timeout. Zero value is ignored.
	Timeout time.Duration

	// Runner can optionally use this to run against a particular snapshot. Empty value is ignored.
	SnapshotId string

	// TODO(jschiller): get consensus on design and either implement or delete.
	// Runner can optionally use this to specify content if creating a new snapshot.
	// Keys: relative src file & dir paths in SnapshotId checkout. May contain '*' wildcard.
	// Values: relative dest path=dir/base in new snapshot (if src has a wildcard, then dest path is treated as a parent dir).
	//
	// Note: nil and empty maps are different!, nil means don't filter, empty means filter everything.
	// SnapshotPlan map[string]string
}

func (c Command) String() string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "Command\n\tSnapshot ID:\t%s\n\tArgv:\t%q\n\tTimeout:\t%v\n",
		c.SnapshotId,
		c.Argv,
		c.Timeout)

	if len(c.EnvVars) > 0 {
		fmt.Fprintf(&b, "\tEnv:\n")
		for k, v := range c.EnvVars {
			fmt.Fprintf(&b, "\t\t%s: %s\n", k, v)
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
