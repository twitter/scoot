package cli

/**
implements the command line entry for the kill job command
*/

import (
	"encoding/json"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/twitter/scoot/common/client"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
)

type killJobCmd struct {
	printAsJson bool
}

func (c *killJobCmd) RegisterFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "kill_job",
		Short: "KillJob",
	}
	r.Flags().BoolVar(&c.printAsJson, "json", false, "Print out job status as JSON")
	return r
}

func (c *killJobCmd) Run(cl *client.SimpleClient, cmd *cobra.Command, args []string) error {

	log.Info("Killing Scoot Job", args)

	if len(args) == 0 {
		return errors.New("a job id must be provided")
	}

	jobId := args[0]

	status, err := cl.ScootClient.KillJob(jobId)

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
