package client

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"log"
)

type Client struct {
	rootCmd          *cobra.Command
	addr             string // modified by flag parsing
	transportFactory thrift.TTransportFactory
	protocolFactory  thrift.TProtocolFactory
	client           *scoot.CloudScootClient
}

func (c *Client) Exec() error {
	return c.rootCmd.Execute()
}

func NewClient(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) (*Client, error) {
	r := &Client{}
	r.transportFactory = transportFactory
	r.protocolFactory = protocolFactory

	rootCmd := &cobra.Command{
		Use:                "scootapi",
		Short:              "scootapi is a command-line client to Cloud Scoot",
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: r.Close,
	}

	r.rootCmd = rootCmd

	rootCmd.AddCommand(makeRunJobCmd(r))
	rootCmd.AddCommand(makeGetStatusCmd(r))
	rootCmd.AddCommand(makeSmokeTestCmd(r))
	return r, nil
}

func (c *Client) Dial() (*scoot.CloudScootClient, error) {
	if c.client == nil {
		if c.addr == "" {
			return nil, fmt.Errorf("Cannot dial: no address")
		}
		log.Println("Dialing", c.addr)
		var transport thrift.TTransport
		transport, err := thrift.NewTSocket(c.addr)
		if err != nil {
			return nil, fmt.Errorf("Error opening socket: %v", err)
		}
		transport = c.transportFactory.GetTransport(transport)
		err = transport.Open()
		if err != nil {
			return nil, fmt.Errorf("Error opening transport: %v", err)
		}
		c.client = scoot.NewCloudScootClientFactory(transport, c.protocolFactory)
	}
	return c.client, nil
}

func (c *Client) Close(cmd *cobra.Command, args []string) error {
	if c.client != nil {
		return c.client.Transport.Close()
	}
	return nil
}
