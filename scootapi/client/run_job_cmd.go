package client

import (
	"fmt"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"log"
)

func makeRunJobCmd(c *Client) *cobra.Command {
	r := &cobra.Command{
		Use:   "run_job",
		Short: "run a job",
		RunE:  c.runJob,
	}
	r.Flags().StringVar(&c.addr, "addr", "localhost:9090", "address to connect to")
	return r
}

func (c *Client) runJob(cmd *cobra.Command, args []string) error {
	log.Println("Running on scoot", args)

	client, err := c.Dial()
	if err != nil {
		return err
	}
	task := scoot.NewTask()
	task.Command = scoot.NewCommand()
	task.Command.Argv = args
	task.SnapshotId = new(string)
	task.ID = new(string)
	*task.ID = "task1"

	jobDef := scoot.NewJobDefinition()
	jobDef.Tasks = append(jobDef.Tasks, task)
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
