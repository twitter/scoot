package cli

/**
implements the command line entry for the throttle scheduler command
*/

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/twitter/scoot/common/client"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
)

type setSchedulerStatusCmd struct {
	printAsJson bool
	maxTasks    int
}

func (c *setSchedulerStatusCmd) RegisterFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "set_scheduler_status",
		Short: "SetSchedulerStatus",
	}
	r.Flags().BoolVar(&c.printAsJson, "json", false, "Print out job status as JSON")
	r.Flags().IntVar(&c.maxTasks, "task-throttle", -1, "Set the task throttle")
	return r
}

func (c *setSchedulerStatusCmd) Run(cl *client.SimpleClient, cmd *cobra.Command, args []string) error {

	log.Info("Set the maximum number of (running + waiting) tasks we want the scheduler"+
		" to run.  Note: the scheduler does not enforce this limit.  We expect the job"+
		" requestor to adhere to it.", args)

	err := cl.ScootClient.SetSchedulerStatus(int32(c.maxTasks))

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

	return nil
}
