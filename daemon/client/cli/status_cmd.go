package cli

import (
	"fmt"
	"log"

	"github.com/scootdev/scoot/runner"
	"github.com/spf13/cobra"
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
	conn, err := c.comms.Dial()
	if err != nil {
		return err
	}
	process, err := conn.Status(runner.RunId(args[0]))
	if err != nil {
		return err
	}
	log.Printf("Status of %v is %v", process.RunId, process.State) // this ends up on stderr
	fmt.Printf("%s", process.State)                                // this ends up on stdout
	return nil
}
