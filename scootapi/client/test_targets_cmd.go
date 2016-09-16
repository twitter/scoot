package client

import (
	"fmt"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"log"
)

func makeTestTargetsCmd(c *Client) *cobra.Command {
	var snapshotID string
	r := &cobra.Command{
		Use:   "test_targets",
		Short: "Test targets",
	}
	r.Flags().StringVar(&c.addr, "addr", "localhost:9090", "address to connect to")
	r.Flags().StringVar(&snapshotID, "snapshot_id", scoot.TaskDefinition_SnapshotId_DEFAULT, "snapshot ID to run job against")
	r.RunE = func(cmd *cobra.Command, args []string) error {
		log.Println(args)
		return c.testTargets(cmd, snapshotID)
	}
	return r
}

func (c *Client) testTargets(cmd *cobra.Command, snapshotID string) error {

	client, err := c.Dial()
	if err != nil {
		return err
	}
	task := scoot.NewTaskDefinition()
	task.Command = scoot.NewCommand()
	for _, t := range getTargets() {
		task.Command.Argv = append(task.Command.Argv, "./pants test "+t+":")
	}
	log.Println("Testing targets", task.Command.Argv)
	task.SnapshotId = &snapshotID
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
			return fmt.Errorf("Error testing targets: %v %T", err, err)
		}
	}
	return nil
}
