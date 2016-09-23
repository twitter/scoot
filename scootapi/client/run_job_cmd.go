package client

import (
	"fmt"
	"github.com/scootdev/scoot/common/thrifthelpers"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"os"
)

type runJobCmd struct {
	snapshotId  string
	jobFilePath string
}

func (c *runJobCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "run_job",
		Short: "run a job",
	}
	r.Flags().StringVar(&c.snapshotId, "snapshot_id", scoot.TaskDefinition_SnapshotId_DEFAULT, "snapshot ID to run job against")
	r.Flags().StringVar(&c.jobFilePath, "job_file_path", "", "file to read targets from")
	return r
}

func (c *runJobCmd) run(cl *Client, cmd *cobra.Command, args []string) error {
	log.Println("Running on scoot", args)

	client, err := cl.Dial()
	if err != nil {
		return err
	}

	jobDef := scoot.NewJobDefinition()
	switch {
	case len(args) > 0 && c.jobFilePath == "":
		task := scoot.NewTaskDefinition()
		task.Command = scoot.NewCommand()
		task.Command.Argv = args
		task.SnapshotId = &c.snapshotId

		jobDef.Tasks = map[string]*scoot.TaskDefinition{
			"task1": task,
		}
	case len(args) == 0 && c.jobFilePath != "":
		f, err := os.Open(t.jobFilePath)
		asBytes, err := ioutil.ReadAll(f)
		if err != nil {
			return err
		}
		thrifthelpers.JsonDeserialize(jobDef, asBytes)
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
