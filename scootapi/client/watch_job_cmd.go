package client

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
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

	log.Println("Watching job:", args)

	if args == nil || len(args) == 0 {
		return errors.New("a job id must be provided")
	}

	err := cl.Dial()
	if err != nil {
		return err
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

func GetAndPrintStatus(jobId string, thriftClient *scoot.CloudScootClient) (*scoot.Status, error) {

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
	asJson, _ := json.MarshalIndent(jobStatus, "", "  ")
	fmt.Printf("%s\n", asJson)
}
