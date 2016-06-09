package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"strings"
)

func makeEchoCmd(c *CliClient) *cobra.Command {
	return &cobra.Command{
		Use:   "echo",
		Short: "echo a command through the Scoot server",
		RunE:  c.echo,
	}
}

func (c *CliClient) echo(cmd *cobra.Command, args []string) error {
	arg := strings.Join(args, " ")
	conn, err := c.comms.Dial()
	if err != nil {
		return err
	}
	result, err := conn.Echo(arg)
	if err != nil {
		return err
	}
	fmt.Println(result)
	return nil
}
