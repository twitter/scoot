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
	conn := c.conn
	c.conn = nil
	return conn.Close()
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
	}

	r := &CliClient{rootCmd, dialer, nil}
	rootCmd.AddCommand(makeEchoCmd(r))
	return r, nil
}
