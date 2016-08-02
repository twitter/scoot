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

	rootCmd.AddCommand(makeQueryworkerCmd(r))
	rootCmd.AddCommand(makeRunCmd(r))
	rootCmd.AddCommand(makeAbortCmd(r))

	return r
}
