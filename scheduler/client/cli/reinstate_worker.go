package cli

/**
implements the command line entry for the reinstate worker job command
*/

import (
	"fmt"
	"os/user"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/twitter/scoot/common/client"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
)

type reinstateWorkerCmd struct {
}

func (c *reinstateWorkerCmd) RegisterFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "reinstate_worker",
		Short: "ReinstateWorker",
	}
	return r
}

func (c *reinstateWorkerCmd) Run(cl *client.SimpleClient, cmd *cobra.Command, args []string) error {

	log.Infof("Reinstating Scoot Worker %s", args)

	if len(args) == 0 {
		return fmt.Errorf("A worker id must be provided in order to reinstate")
	}

	id := args[0]
	requestor, err := user.Current()
	if err != nil {
		return err
	}

	err = cl.ScootClient.ReinstateWorker(&scoot.ReinstateWorkerReq{ID: id, Requestor: requestor.Username})

	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return fmt.Errorf("Invalid Request: %v", err.GetMessage())
		case *scoot.ScootServerError:
			return fmt.Errorf("Scoot server error: %v", err.Error())
		default:
			return fmt.Errorf("Error reinstating worker: %v", err.Error())
		}
	}

	log.Infof("Worker %s reinstated", id)

	return nil
}
