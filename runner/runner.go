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

// TODO(dbentley): do we want to have Runner interface?
// It combines a few things that are related, but different responsibilities.
// Perhaps we could rename it something like "RunService", to make clear it's both ends?
// type Runner interface {
// 	Controller
// 	LegacyStatuses
// }
