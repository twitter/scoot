package client

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/spf13/cobra"
)

type CliClient interface {
	Cli() error
}

type cliClient struct {
	client  client
	rootCmd *cobra.Command
}

func (c *cliClient) Cli() error {
	return c.rootCmd.Execute()
}

func NewCliClient(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) CliClient {
	r := &cliClient{}
	r.client.transportFactory = transportFactory
	r.client.protocolFactory = protocolFactory
	// r.client.addr is provided as a cmdline flag.

	rootCmd := &cobra.Command{
		Use:                "workercl",
		Short:              "workercl is a command-line client to Cloud Worker",
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: func(*cobra.Command, []string) error { return r.client.Close() },
	}

	r.rootCmd = rootCmd

	r.addCmd(&runCmd{client: &r.client}, &cobra.Command{
		Use:   "run",
		Short: "runs a command",
	})
	r.addCmd(&abortCmd{client: &r.client}, &cobra.Command{
		Use:   "abort",
		Short: "aborts a runId",
	})
	r.addCmd(&queryWorkerCmd{client: &r.client}, &cobra.Command{
		Use:   "queryworker",
		Short: "queries worker status",
	})

	return r
}

func (c *cliClient) addCmd(cmd cmd, cobraCmd *cobra.Command) {
	cobraCmd.Flags().StringVar(&c.client.addr, "addr", "localhost:9090", "worker server address")
	cmd.registerFlags(cobraCmd)
	cobraCmd.RunE = cmd.run
	c.rootCmd.AddCommand(cobraCmd)
}

type cmd interface {
	registerFlags(cmd *cobra.Command)
	run(cmd *cobra.Command, args []string) error
}
