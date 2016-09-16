package client

import (
	"fmt"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"log"
)

func makeTestTargetsCmd(c *Client) *cobra.Command {
	r := &cobra.Command{
		Use:   "test_targets",
		Short: "Test targets",
	}
	r.Flags().StringVar(&c.addr, "addr", "localhost:9090", "address to connect to")
	r.RunE = func(cmd *cobra.Command, args []string) error {
		return c.testTargets(cmd, args)
	}
	return r
}

func (c *Client) testTargets(cmd *cobra.Command, args []string) error {
	client, err := c.Dial()
	if err != nil {
		return err
	}
	jobDef := scoot.NewJobDefinition()
	jobDef.Tasks = make(map[string]*scoot.TaskDefinition)
	// create a task for each target to test
	for _, t := range getTargets() {
		task := scoot.NewTaskDefinition()
		task.Command = scoot.NewCommand()
		task.Command.Argv = []string{"./pants", "test", t}
		jobDef.Tasks["task-"+t] = task
	}
	log.Println("jobDef:", jobDef)
	id, err := client.RunJob(jobDef)
	log.Println("JobID:", id)
	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return fmt.Errorf("Invalid Request: %v", err.GetMessage())
		default:
			return fmt.Errorf("Error testing targets: %v %T", err, err)
		}
	}
	return nil
}
