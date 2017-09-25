package client

import (
	log "github.com/sirupsen/logrus"
	"time"

	"github.com/spf13/cobra"
	"github.com/twitter/scoot/runner"
)

// Run
type runCmd struct {
	client *simpleClient

	// Flags
	snapshotID string
	timeout    time.Duration
}

func (rc *runCmd) registerFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&rc.snapshotID, "snapshotid", "", "snapshot/patch id.")
	cmd.Flags().DurationVar(&rc.timeout, "timeout", 0, "how long to let the command run (0 for infinite)")
}

func (rc *runCmd) run(cmd *cobra.Command, args []string) error {
	cmdToRun := &runner.Command{
		Argv:       args,
		Timeout:    rc.timeout,
		SnapshotID: rc.snapshotID,
	}
	log.Infof("Calling run RPC to Cloud Worker:\n%s", cmdToRun)

	status, err := rc.client.Run(cmdToRun)
	log.Infof("%v\nError: %v\n", status, err)
	return nil
}

// Abort
type abortCmd struct {
	client *simpleClient

	// Flags
	runId string
}

func (ac *abortCmd) registerFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&ac.runId, "id", "", "run id to abort")
}

func (ac *abortCmd) run(cmd *cobra.Command, args []string) error {
	log.Info("Calling abort rpc to cloud worker", args)

	status, err := ac.client.Abort(runner.RunID(ac.runId))
	log.Infof("%v\nError: %v\n", status, err)
	return nil
}

// QueryWorker
type queryWorkerCmd struct {
	client *simpleClient
}

func (qc *queryWorkerCmd) registerFlags(cmd *cobra.Command) {}

func (qc *queryWorkerCmd) run(cmd *cobra.Command, args []string) error {
	log.Info("Calling queryworker rpc to cloud worker", args)

	status, err := qc.client.QueryWorker()
	log.Infof("%v\nError: %v\n", status, err)
	return nil
}

//TODO: implement Erase()
