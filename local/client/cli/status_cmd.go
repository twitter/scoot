package cli

import (
	"fmt"
	"github.com/scootdev/scoot/runner"
	"github.com/spf13/cobra"
	"log"
)

func makeStatusCmd(c *CliClient) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "find status for a command",
		RunE:  c.status,
	}
}

func (c *CliClient) status(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("Must pass exactly one arg of RunId to query; got %v", len(args))
	}
	err := c.comms.Open()
	if err != nil {
		return err
	}
	process, err := c.comms.Status(runner.RunId(args[0]))
	if err != nil {
		return err
	}
	log.Printf("Status of %v is %v", process.RunId, process.State)
	return nil
}
