package client

import (
	"github.com/spf13/cobra"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/scootapi"
)

// Worker API Client interface that includes CLI handling
type CLIClient interface {
	Exec() error
}

// Basic CLIClient implementation that uses simpleClient
type simpleCLIClient struct {
	client  simpleClient
	rootCmd *cobra.Command
}

func (c *simpleCLIClient) Exec() error {
	return c.rootCmd.Execute()
}

func NewSimpleCLIClient(di dialer.Dialer) (CLIClient, error) {
	c := &simpleCLIClient{}
	c.client.dialer = di
	// c.client.addr is provided as a cmdline flag.

	rootCmd := &cobra.Command{
		Use:                "workercl",
		Short:              "workercl is a command-line client to Cloud Worker",
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: func(*cobra.Command, []string) error { return c.client.Close() },
	}

	c.rootCmd = rootCmd

	c.addCmd(&runCmd{client: &c.client}, &cobra.Command{
		Use:   "run",
		Short: "runs a command",
	})
	c.addCmd(&abortCmd{client: &c.client}, &cobra.Command{
		Use:   "abort",
		Short: "aborts a runId",
	})
	c.addCmd(&queryWorkerCmd{client: &c.client}, &cobra.Command{
		Use:   "queryworker",
		Short: "queries worker status",
	})

	return c, nil
}

func (c *simpleCLIClient) addCmd(cmd command, cobraCmd *cobra.Command) {
	cobraCmd.Flags().StringVar(&c.client.addr, "addr", scootapi.DefaultWorker_Thrift, "worker server address")
	cmd.registerFlags(cobraCmd)
	cobraCmd.RunE = cmd.run
	c.rootCmd.AddCommand(cobraCmd)
}

type command interface {
	registerFlags(cmd *cobra.Command)
	run(cmd *cobra.Command, args []string) error
}
