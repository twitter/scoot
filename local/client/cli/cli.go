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

func (c *CliClient) Close(*cobra.Command, []string) error {
	if c.conn == nil {
		return nil
	}
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
	r := &CliClient{nil, dialer, nil}

	rootCmd := &cobra.Command{
		Use:                "scootcl",
		Short:              "Scootcl is a command-line client to Scoot",
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: r.Close,
	}

	r.rootCmd = rootCmd

	rootCmd.AddCommand(makeEchoCmd(r))
	rootCmd.AddCommand(makeRunCmd(r))
	rootCmd.AddCommand(makeStatusCmd(r))
	return r, nil
}
