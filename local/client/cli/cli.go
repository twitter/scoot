package cli

import (
	"github.com/scootdev/scoot/local/client/conn"
	"github.com/spf13/cobra"
)

type CliClient struct {
	rootCmd *cobra.Command
	dialer  conn.Dialer
	conn    conn.Conn
}

func (c *CliClient) Exec() error {
	return c.rootCmd.Execute()
}

func (c *CliClient) Close() error {
	return c.conn.Close()
}

func (c *CliClient) openConn() (conn.Conn, error) {
	if c.conn == nil {
		conn, err := c.dialer.Dial()
		if err != nil {
			return nil, err
		}
		c.conn = conn
	}
	return c.conn, nil
}

func NewCliClient(dialer conn.Dialer) (*CliClient, error) {
	rootCmd := &cobra.Command{
		Use:   "scootcl",
		Short: "Scootcl is a command-line client to Scoot",
		// Run: func(cmd *cobra.Command, args []string) {
		// 	log.Println("Scoot does nothing")
		// },
	}

	r := &CliClient{rootCmd, dialer, nil}
	rootCmd.AddCommand(makeEchoCmd(r))
	return r, nil
}
