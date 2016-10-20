package server

// implement the Scoot API Run requests for local processing

import (
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/snapshot"

	"fmt"
	"time"
)

const maxAccumulatedWaitTime time.Duration = 1 * time.Minute //TODO parameterize this

type LocalScoot struct {
	runners          []runner.Runner
	runIdToRunnerMap map[runner.RunId]runner.Runner // tracks which runid is being run by which runner
}

func NewLocalScoot(numRunners int, exec execer.Execer, checkouter snapshot.Checkouter, outputCreator runner.OutputCreator) *LocalScoot {
	localScoot := LocalScoot{}

	localScoot.runners = make([]runner.Runner, numRunners)
	localScoot.runIdToRunnerMap = make(map[runner.RunId]runner.Runner)

	for i := 0; i < numRunners; i++ {
		localScoot.runners[i] = (runner.Runner)(local.NewSimpleRunner(exec, checkouter, outputCreator))
	}

	return &localScoot
}

// find a runner that can run the requested command.  If all runners are currently processing commands
// return an error, otherwise have the runner run the requested command.
func (r *LocalScoot) Run(snapshotId string, cmd RunCommand, outputStrategy OutputStrategy) (runner.RunId, error) {

	for _, runnerCandidate := range r.runners {
		// loop through runners looking for an available runner

		var env map[string]string
		runnerCommand := runner.NewCommand(cmd, env, maxAccumulatedWaitTime, snapshotId)

		// start the command
		status, err := runnerCandidate.Run(runnerCommand)

		if err == nil {
			// if it started successfully, stop looking
			r.runIdToRunnerMap[status.RunId] = runnerCandidate
			return status.RunId, nil
		}
	}

	return "", fmt.Errorf("No runners available")
}
