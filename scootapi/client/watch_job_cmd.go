package client

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
)

const (
	jobStatusSleepSeconds time.Duration = 3 * time.Second
)

type watchJobCmd struct {
	jobId string
}

func (c *watchJobCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "watch_job",
		Short: "Watch job",
	}

	return r
}

func (c *watchJobCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {

	log.Info("Watching job:", args)

	if args == nil || len(args) == 0 {
		return errors.New("a job id must be provided")
	}

	jobId := args[0]

	for {
		jobStatus, err := GetAndPrintStatus(jobId, cl.scootClient)
		if err != nil {
			return err
		}

		if *jobStatus == scoot.Status_COMPLETED || *jobStatus == scoot.Status_ROLLED_BACK {
			return nil
		}

		time.Sleep(jobStatusSleepSeconds)
	}

}

func GetAndPrintStatus(jobId string, thriftClient scoot.CloudScoot) (*scoot.Status, error) {

	status, err := thriftClient.GetStatus(jobId)
	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return nil, fmt.Errorf("Invalid Request: %v", err.GetMessage())
		case *scoot.ScootServerError:
			return nil, fmt.Errorf("Error getting status: %v", err.Error())
		}
	}
	PrintJobStatus(status)

	return &status.Status, nil

}

func PrintJobStatus(jobStatus *scoot.JobStatus) {
	log.Infof("Job id: %s\n", jobStatus.ID)
	log.Infof("Job status: %s\n", jobStatus.Status.String())
	for taskId, taskStatus := range jobStatus.TaskStatus {
		log.Infof("\tTask %s {\n", taskId)
		log.Infof("\t\tStatus: %s\n", taskStatus.String())
		runStatus, ok := jobStatus.TaskData[taskId]
		if ok {
			if runStatus.OutUri != nil {
				log.Infof("\t\tStdout: %v\n", *runStatus.OutUri)
			}
			if runStatus.ErrUri != nil {
				log.Infof("\t\tStderr: %v\n", *runStatus.ErrUri)
			}
			if runStatus.SnapshotId != nil {
				log.Infof("\t\tSnapshot: %v\n", *runStatus.SnapshotId)
			}
			if taskStatus == scoot.Status_COMPLETED {
				if runStatus.ExitCode != nil {
					exitCode := *runStatus.ExitCode
					log.Infof("\t\tExitCode: %d\n", exitCode)
				}
				if runStatus.Error != nil {
					log.Infof("\t\tError: %v\n", *runStatus.Error)
				}
			}

		}

		log.Infof("\t}\n")
	}
}
