package client

import (
	"fmt"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"log"
)

func makeRunJobCmd(c *Client) *cobra.Command {
	var snapshotId string
	r := &cobra.Command{
		Use:   "run_job",
		Short: "run a job",
	}
	r.Flags().StringVar(&c.addr, "addr", "localhost:9090", "address to connect to")
	r.Flags().StringVar(&snapshotId, "snapshot_id", scoot.TaskDefinition_SnapshotId_DEFAULT, "snapshot ID to run job against")
	r.RunE = func(cmd *cobra.Command, args []string) error {
		return c.runJob(cmd, args, snapshotId)
	}
	return r
}

func (c *Client) runJob(cmd *cobra.Command, args []string, snapshotId string) error {
	log.Println("Running on scoot", args)

	client, err := c.Dial()
	if err != nil {
		return err
	}
	task := scoot.NewTaskDefinition()
	task.Command = scoot.NewCommand()
	task.Command.Argv = args
	task.SnapshotId = &snapshotId
	jobDef := scoot.NewJobDefinition()
	jobDef.Tasks = map[string]*scoot.TaskDefinition{
		"task1": task,
	}
	_, err = client.RunJob(jobDef)
	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return fmt.Errorf("Invalid Request: %v", err.GetMessage())
		default:
			return fmt.Errorf("Error running job: %v %T", err, err)
		}
	}
	return nil
}
