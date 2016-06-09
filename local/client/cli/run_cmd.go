package cli

import (
	"github.com/scootdev/scoot/runner"
	"github.com/spf13/cobra"
	"log"
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
	command := runner.NewCommand(args, map[string]string{}, 0)
	process, err := conn.Run(command)
	if err != nil {
		return err
	}
	log.Println("Running as : ", process.RunId)
	return nil
}
