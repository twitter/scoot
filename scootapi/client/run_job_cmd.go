package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"os"

	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
)

type runJobCmd struct {
	streamName  string
	snapshotId  string
	jobFilePath string
}

func (c *runJobCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "run_job",
		Short: "run a job",
	}
	r.Flags().StringVar(&c.streamName, "stream_name", "sm", "If passing snapshot_id=SHA, this is the global UID for the associated repo.")
	r.Flags().StringVar(&c.snapshotId, "snapshot_id", "", "Repo checkout id: <master-sha> OR <backend>-<kind>(-<additional information>)+")
	r.Flags().StringVar(&c.jobFilePath, "job_def", "", "JSON file to read jobs from. Error if snapshot_id flag is also provided.")
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
	log.Info("Running on scoot", args)

	jobDef := scoot.NewJobDefinition()
	switch {
	case len(args) > 0 && c.jobFilePath != "":
		return errors.New("You must provide either args or a job definition")
	case len(args) > 0:
		if c.snapshotId == "" {
			log.Info("No snapshotID provided - cmd will be run in an empty tmpdir.")
		} else if !strings.Contains(c.snapshotId, "-") {
			//this is not a bundleID, assume it's a sha that's available upstream. Cf. snapshot/git/gitdb/README.md
			streamId := fmt.Sprintf("stream-swh-%s-%s", c.streamName, c.snapshotId)
			log.Infof("Converting sha to a stream-based snapshot_id: %s -> %s", c.snapshotId, streamId)
			c.snapshotId = streamId
		}
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

	log.Info(jobId.ID)
	log.Infof("JobID:%s\n", jobId.ID)

	return nil
}
