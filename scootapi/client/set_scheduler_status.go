package client

/**
implements the command line entry for the throttle scheduler command
*/

import (
	"errors"
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
)

type setSchedulerStatus struct {
	printAsJson bool
}

func (c *setSchedulerStatus) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:     "set_scheduler_status",
		Short:   "set the scheduler status",
		Example: "scootapi set_scheduler_status 20",
	}
	r.Flags().BoolVar(&c.printAsJson, "json", false, "Print out job status as JSON")
	return r
}

func (c *setSchedulerStatus) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {

	log.Info("Set the maximum number of (running + waiting) tasks the scheduler will allow.", args)

	if len(args) == 0 {
		return errors.New("tasks limit >= -1 must be provided.")
	}

	limit, err := strconv.Atoi(args[0])
	if err != nil || limit < -1 {
		return fmt.Errorf("Invalid input. Tasks limit must be an integer > -1.")
	}

	err = cl.scootClient.SetSchedulerStatus(int32(limit))

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
