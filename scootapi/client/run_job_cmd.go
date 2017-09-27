package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/twitter/scoot/scootapi/gen-go/scoot"
)

type runJobCmd struct {
	streamName  string
	snapshotId  string
	jobFilePath string
	tag         string
}

func (c *runJobCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "run_job",
		Short: "run a job",
	}
	r.Flags().StringVar(&c.streamName, "stream_name", "sm", "If passing snapshot_id=SHA, this is the global UID for the associated repo.")
	r.Flags().StringVar(&c.snapshotId, "snapshot_id", "", "Repo checkout id: <master-sha> OR <backend>-<kind>(-<additional information>)+")
	r.Flags().StringVar(&c.jobFilePath, "job_def", "", "JSON file to read jobs from. Error if snapshot_id flag is also provided.")
	r.Flags().StringVar(&c.tag, "tag", "", "Tag can be specified by requestor in order to more easily trace a job through logs")
	return r
}

// Scoot JobDefinitions specified in job_def JSON files should be able to
// satisfy these types as populated via https://golang.org/pkg/encoding/json/#Unmarshal
type CLIJobDef struct {
	Tasks                []TaskDef
	DefaultTaskTimeoutMs int32
	Priority             int32
	Tag                  string
	Basis                string
	JobType              string
	Requestor            string
}

type TaskDef struct {
	Args       []string
	SnapshotID string
	TimeoutMs  int32
	TaskID     string
}

func (c *runJobCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {
	log.Info("Running on scoot, args:", args)
	jobDef := scoot.NewJobDefinition()
	jobDef.Tag = &c.tag
	switch {
	case len(args) > 0 && c.jobFilePath != "":
		return errors.New("You must provide either args or a job definition")
	case len(args) > 0:
		if c.snapshotId == "" {
			log.Info("No snapshotID provided - cmd will be run in an empty tmpdir.")
		} else if !strings.Contains(c.snapshotId, "-") {
			//this is not a bundleID, assume it's a sha that's available upstream. Cf. snapshot/git/gitdb/README.md
			streamId := fmt.Sprintf("stream-gc-%s-%s", c.streamName, c.snapshotId)
			log.Infof("Converting sha to a stream-based snapshot_id: %s -> %s", c.snapshotId, streamId)
			c.snapshotId = streamId
		}
		task := scoot.NewTaskDefinition()
		taskId := "task1"
		task.Command = scoot.NewCommand()
		task.Command.Argv = args
		task.SnapshotId = &c.snapshotId
		task.TaskId = &taskId
		jobDef.Tasks = []*scoot.TaskDefinition{task}
	case c.jobFilePath != "":
		f, err := os.Open(c.jobFilePath)
		if err != nil {
			return err
		}
		asBytes, err := ioutil.ReadAll(f)
		if err != nil {
			return err
		}
		var jsonJob CLIJobDef
		err = json.Unmarshal(asBytes, &jsonJob)
		if err != nil {
			return err
		}

		if jsonJob.DefaultTaskTimeoutMs > 0 {
			jobDef.DefaultTaskTimeoutMs = &jsonJob.DefaultTaskTimeoutMs
		}
		jobDef.Tag = &jsonJob.Tag
		jobDef.Basis = &jsonJob.Basis
		jobDef.JobType = &jsonJob.JobType
		jobDef.Requestor = &jsonJob.Requestor
		jobDef.Priority = &jsonJob.Priority
		jobDef.Tasks = []*scoot.TaskDefinition{}
		for _, jsonTask := range jsonJob.Tasks {
			jt := jsonTask
			taskDef := scoot.NewTaskDefinition()
			taskDef.Command = scoot.NewCommand()
			taskDef.Command.Argv = jt.Args
			taskDef.SnapshotId = &jt.SnapshotID
			taskDef.TaskId = &jt.TaskID
			jobDef.Tasks = append(jobDef.Tasks, taskDef)
			if jt.TimeoutMs > 0 {
				taskDef.TimeoutMs = &jt.TimeoutMs
			}
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

	fmt.Println(jobId.ID) // must go to std out in case caller looking in stdout for the results
	log.Infof("JobID: %s", jobId.ID)

	return nil
}
