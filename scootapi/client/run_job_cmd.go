package client

import (
	"fmt"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"log"
)

type runJobCmd struct {
	snapshotId string
}

func (c *runJobCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "run_job",
		Short: "run a job",
	}
	r.Flags().StringVar(&c.snapshotId, "snapshot_id", scoot.TaskDefinition_SnapshotId_DEFAULT, "snapshot ID to run job against")
	return r
}

func (c *runJobCmd) run(cl *Client, cmd *cobra.Command, args []string) error {
	log.Println("Running on scoot", args)

	client, err := cl.Dial()
	if err != nil {
		return err
	}
	task := scoot.NewTaskDefinition()
	task.Command = scoot.NewCommand()
	task.Command.Argv = args
	task.SnapshotId = &c.snapshotId
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
