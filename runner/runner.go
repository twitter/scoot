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

// A command, execution environment, and timeout.
type Command struct {
	// Command line to run
	Argv []string

	// Key-value pairs for environment variables
	EnvVars map[string]string

	// Kill command after timeout. Zero value is ignored.
	Timeout time.Duration

	// Runner can optionally use this to run against a particular snapshot. Empty value is ignored.
	SnapshotID string

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
	fmt.Fprintf(&b, "Command\n\tSnapshotID:\t%s\n\tArgv:\t%q\n\tTimeout:\t%v\n",
		c.SnapshotID,
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

// XXX(dbentley): remove this
// func NewCommand(argv []string, env map[string]string, timeout time.Duration) *Command {
// 	return &Command{Argv: argv, EnvVars: env, Timeout: timeout}
// }

// Service allows starting/abort'ing runs and checking on their status.
type Service interface {
	Controller
	StatusReader

	// We need StatusEraser because Erase() shouldn't be part of StatusReader,
	// but we have it for legacy reasons.
	StatusEraser
}

// XXX(dbentley): remove Runner
// Alias that exists just to make refactor PR smaller.
type Runner interface {
	Service
}
