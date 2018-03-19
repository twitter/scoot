package client

/**
implements the command line entry for the reinstate worker job command
*/

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
)

type reinstateWorkerCmd struct {
}

func (c *reinstateWorkerCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "reinstate_worker",
		Short: "reinstateWorker",
	}
	return r
}

func (c *reinstateWorkerCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {

	log.Infof("Offlining Scoot Worker %s", args)

	if len(args) == 0 {
		return fmt.Errorf("A worker id must be provided in order to reinstate")
	}

	id := args[0]

	err := cl.scootClient.ReinstateWorker(id)

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

	log.Info("Worker %s reinstated", id)

	return nil
}
