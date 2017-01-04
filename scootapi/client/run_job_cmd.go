package client

import (
	"encoding/json"
	"errors"
	"fmt"

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
	r.Flags().StringVar(&c.jobFilePath, "job_def", "", "JSON file to read jobs from")
	return r
}

// Types to handle JobDefinitions from JSON files
type JobDef struct {
	Tasks map[string]TaskDef
}
type TaskDef struct {
	Args       []string
	SnapshotID string
}

func (c *runJobCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {
	log.Println("Running on scoot", args)

	err := cl.Dial()
	if err != nil {
		return err
	}

	jobDef := scoot.NewJobDefinition()
	switch {
	case len(args) > 0 && c.jobFilePath != "":
		return errors.New("You must provide either args or a job definition")
	case len(args) > 0:
		task := scoot.NewTaskDefinition()
		task.Command = scoot.NewCommand()
		task.Command.Argv = args
		task.SnapshotId = &c.snapshotId

		jobDef.Tasks = map[string]*scoot.TaskDefinition{
			"task1": task,
		}
	case c.jobFilePath != "":
		f, err := os.Open(c.jobFilePath)
		if err != nil {
			return err
		}
		asBytes, err := ioutil.ReadAll(f)
		if err != nil {
			return err
		}
		var jsonJob JobDef
		err = json.Unmarshal(asBytes, &jsonJob)
		if err != nil {
			return err
		}

		jobDef.Tasks = make(map[string]*scoot.TaskDefinition)
		for taskName, jsonTask := range jsonJob.Tasks {
			taskDef := scoot.NewTaskDefinition()
			taskDef.Command = scoot.NewCommand()
			taskDef.Command.Argv = jsonTask.Args
			taskDef.SnapshotId = &jsonTask.SnapshotID
			jobDef.Tasks[taskName] = taskDef
		}
	}
	jobId, err := cl.scootClient.RunJob(jobDef)
	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return fmt.Errorf("Invalid Request: %v", err.GetMessage())
		default:
			return fmt.Errorf("Error running job: %v %T", err, err)
		}
	}

	fmt.Println(jobId.ID)
	log.Printf("JobID:%s\n", jobId.ID)

	return nil
}
