package cli

/**
implements the command line entry for the get scheduler status command
*/

import (
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/wisechengyi/scoot/common/client"
	"github.com/wisechengyi/scoot/scheduler/api/thrift/gen-go/scoot"
)

type getSchedulerStatusCmd struct {
	printAsJson bool
}

func (c *getSchedulerStatusCmd) RegisterFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "get_scheduler_status",
		Short: "GetSchedulerStatus",
	}
	r.Flags().BoolVar(&c.printAsJson, "json", false, "Print out status as JSON")
	return r
}

func (c *getSchedulerStatusCmd) Run(cl *client.SimpleClient, cmd *cobra.Command, args []string) error {

	log.Info("Checking Status for Scheduler", args)

	status, err := cl.ScootClient.GetSchedulerStatus()

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
