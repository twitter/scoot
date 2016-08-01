package client

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/spf13/cobra"
)

type cliClient struct {
	client
	rootCmd *cobra.Command
}

func (c *cliClient) Cli() error {
	return c.rootCmd.Execute()
}

func NewCliClient(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) Client {
	r := &cliClient{}
	r.transportFactory = transportFactory
	r.protocolFactory = protocolFactory
	// r.addr is provided as a cmdline flag.

	rootCmd := &cobra.Command{
		Use:                "workercl",
		Short:              "workercl is a command-line client to Cloud Worker",
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: func(*cobra.Command, []string) error { return r.Close() },
	}

	r.rootCmd = rootCmd

	rootCmd.AddCommand(makeQueryworkerCmd(r))
	rootCmd.AddCommand(makeRunCmd(r))
	rootCmd.AddCommand(makeAbortCmd(r))

	return r
}
