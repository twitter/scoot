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

type throttleSchedulerCmd struct {
	printAsJson bool
}

func (c *throttleSchedulerCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "throttle_scheduler",
		Short: "ThrottleScheduler",
	}
	r.Flags().BoolVar(&c.printAsJson, "json", false, "Print out job status as JSON")
	return r
}

func (c *throttleSchedulerCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {

	log.Info("Throttle the scheduler", args)

	if len(args) == 0 {
		return errors.New("a throttle value >= -1 must be provided.")
	}

	limit, err := strconv.Atoi(args[0])
	if err != nil || limit < -1 {
		return fmt.Errorf("Invalid input. Throttle limit must be an integer > -1.")
	}

	err = cl.scootClient.ThrottleScheduler(int32(limit))

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
