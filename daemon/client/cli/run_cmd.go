package cli

import (
	"log"

	"github.com/scootdev/scoot/runner"
	"github.com/spf13/cobra"
)

func makeRunCmd(c *CliClient) *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "run a command",
		RunE:  c.run,
	}
}

func (c *CliClient) run(cmd *cobra.Command, args []string) error {
	log.Println("Running on scoot", args)
	conn, err := c.comms.Dial()
	if err != nil {
		return err
	}
	command := runner.NewCommand(args, map[string]string{}, 0, "")
	process := conn.Run(command)
	log.Printf("Running as %v, status %v", process.RunId, process.State)
	return nil
}
