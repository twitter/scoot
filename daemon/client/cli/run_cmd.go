package cli

import (
	"fmt"
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
		log.Printf("Dial error:%s", err.Error()) // this ends up on stderr
		return err
	}
	command := runner.NewCommand(args, map[string]string{}, 0, "")
	process, err := conn.Run(command)
	if err != nil {
		log.Printf("Run error:%s", err.Error()) // this ends up on stderr
		return err
	}
	log.Printf("Running as %v, status %v", process.RunId, process.State) // this ends up on stderr
	fmt.Printf("%s", process.RunId)                                      // this ends up on stdout
	return nil
}
