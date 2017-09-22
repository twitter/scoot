package client

import (
	"encoding/json"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
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

	log.Info("Checking Status for Scoot Job", args)

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
		log.Infof("%s\n", asJson)
		fmt.Printf("%s\n", asJson) // must also go to stdout in case caller looking in stdout for the results
	} else {
		log.Info("Job Status:", status)
		fmt.Println("Job Status:", status) // must also go to stdout in case caller looking in stdout for the results
	}

	return nil
}
