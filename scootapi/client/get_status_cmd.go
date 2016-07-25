package client

import (
	"fmt"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"log"
)

func makeGetStatusCmd(c *Client) *cobra.Command {
	r := &cobra.Command{
		Use:   "get_job_status",
		Short: "Get Job Status",
		RunE:  c.getStatus,
	}

	r.Flags().StringVar(&c.addr, "addr", "localhost:9090", "address to connect to")
	return r
}

func (c *Client) getStatus(cmd *cobra.Command, args []string) error {

	log.Println("Checking Status for Scoot Job", args)
	client, err := c.Dial()

	if err != nil {
		return err
	}

	jobId := args[0]

	status, err := client.GetStatus(jobId)

	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return fmt.Errorf("Invalid Request: %v", err.GetMessage())
		case *scoot.ScootServerError:
			return fmt.Errorf("Error getting status: %v", err.Error())
		}
	}

	fmt.Println("Job Status: ", status)

	return nil
}
