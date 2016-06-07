package cli

import (
	"github.com/spf13/cobra"
	"log"
	"strings"
)

func makeEchoCmd(cli *CliClient) *cobra.Command {
	return &cobra.Command{
		Use:   "echo",
		Short: "echo a command through the Scoot server",
		RunE:  cli.echo,
	}
}

func (c *CliClient) echo(cmd *cobra.Command, args []string) error {
	arg := strings.Join(args, " ")
	log.Println("Echoing to server", arg)
	conn, err := c.openConn()
	if err != nil {
		return err
	}
	result, err := conn.Echo(arg)
	if err != nil {
		return err
	}
	log.Println("Scoot server: ", result)
	return nil
}
