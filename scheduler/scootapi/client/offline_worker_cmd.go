package client

/**
implements the command line entry for the offline worker job command
*/

import (
	"fmt"
	"os/user"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/twitter/scoot/scheduler/scootapi/gen-go/scoot"
)

type offlineWorkerCmd struct {
}

func (c *offlineWorkerCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "offline_worker",
		Short: "OfflineWorker",
	}
	return r
}

func (c *offlineWorkerCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {

	log.Infof("Offlining Scoot Worker %s", args)

	if len(args) == 0 {
		return fmt.Errorf("A worker id must be provided in order to offline")
	}

	id := args[0]
	requestor, err := user.Current()
	if err != nil {
		return err
	}

	err = cl.scootClient.OfflineWorker(&scoot.OfflineWorkerReq{ID: id, Requestor: requestor.Username})

	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return fmt.Errorf("Invalid Request: %v", err.GetMessage())
		case *scoot.ScootServerError:
			return fmt.Errorf("Scoot server error: %v", err.Error())
		default:
			return fmt.Errorf("Error offlining worker: %v", err.Error())
		}
	}

	log.Infof("Worker %s offlined", id)

	return nil
}
