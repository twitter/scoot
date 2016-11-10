package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"log"
)

type getStatusCmd struct{}

func (c *getStatusCmd) registerFlags() *cobra.Command {
	return &cobra.Command{
		Use:   "get_job_status",
		Short: "GetJobStatus",
	}
}

func (c *getStatusCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {

	log.Println("Checking Status for Scoot Job", args)

	if len(args) == 0 {
		return errors.New("a job id must be provided")
	}

	err := cl.Dial()
	if err != nil {
		return err
	}

	jobId := args[0]

	status, err := cl.scootClient.GetStatus(jobId)

	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return fmt.Errorf("Invalid Request: %v", err.GetMessage())
		case *scoot.ScootServerError:
			return fmt.Errorf("Error getting status: %v", err.Error())
		}
	}

	asJson, err := json.MarshalIndent(status, "", "  ")
	fmt.Printf("%s\n", asJson)

	return nil
}
