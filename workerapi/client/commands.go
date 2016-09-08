package client

import (
	"log"
	"time"

	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/runner"
	"github.com/spf13/cobra"
)

// Run
type runCmd struct {
	client *client

	// Flags
	snapshotID string
	timeout    time.Duration
}

func (c *runCmd) registerFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&c.snapshotID, "snapshotid", "", "snapshot/patch id.")
	cmd.Flags().DurationVar(&c.timeout, "timeout", 0, "how long to let the command run (0 for infinite)")
}

func (c *runCmd) run(cmd *cobra.Command, args []string) error {
	cmdToRun := &runner.Command{
		Argv:       args,
		Timeout:    c.timeout,
		SnapshotId: c.snapshotID,
	}
	log.Println("Calling run rpc to cloud worker", render.Render(cmdToRun))

	status, err := c.client.Run(cmdToRun)
	log.Println(render.Render(status), err)
	return nil
}

// Abort
type abortCmd struct {
	client *client

	runId string
}

func (c *abortCmd) registerFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&c.runId, "id", "", "run id to abort")
}

func (c *abortCmd) run(cmd *cobra.Command, args []string) error {
	log.Println("Calling abort rpc to cloud worker", args)

	status, err := c.client.Abort(c.runId)
	log.Println(render.Render(status), err)
	return nil
}

// QueryWorker
type queryWorkerCmd struct {
	client *client
}

func (c *queryWorkerCmd) registerFlags(cmd *cobra.Command) {}

func (c *queryWorkerCmd) run(cmd *cobra.Command, args []string) error {
	log.Println("Calling queryworker rpc to cloud worker", args)

	status, err := c.client.QueryWorker()
	log.Println(render.Render(status), err)
	return nil
}

//TODO: implement Erase()
