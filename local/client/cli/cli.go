package cli

import (
	"github.com/scootdev/scoot/local/client/conn"
	"github.com/spf13/cobra"
)

type CliClient struct {
	rootCmd *cobra.Command
	comms   conn.Dialer
}

func (c *CliClient) Exec() error {
	return c.rootCmd.Execute()
}

func (c *CliClient) Close(*cobra.Command, []string) error {
	return c.comms.Close()
}

func NewCliClient(dialer conn.Dialer) (*CliClient, error) {
	r := &CliClient{nil, dialer}

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
