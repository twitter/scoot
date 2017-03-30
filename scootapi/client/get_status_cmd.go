package client

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
)

type getStatusCmd struct {
	printAsJson bool
}

func (c *getStatusCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "get_job_status",
		Short: "GetJobStatus",
	}
	r.Flags().BoolVar(&c.printAsJson, "json", false, "Print out status as JSON")
	return r
}

func (c *getStatusCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {

	log.Infoln("Checking Status for Scoot Job", args)

	if len(args) == 0 {
		return errors.New("a job id must be provided")
	}

	jobId := args[0]

	status, err := cl.scootClient.GetStatus(jobId)

	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return fmt.Errorf("Invalid Request: %v", err.GetMessage())
		case *scoot.ScootServerError:
			return fmt.Errorf("Scoot server error: %v", err.Error())
		default:
			return fmt.Errorf("Error getting status: %v", err.Error())
		}
	}

	if c.printAsJson {
		asJson, err := json.Marshal(status)
		if err != nil {
			return fmt.Errorf("Error converting status to JSON: %v", err.Error())
		}
		fmt.Printf("%s\n", asJson)
	} else {
		fmt.Println("Job Status:", status)
	}

	return nil
}
