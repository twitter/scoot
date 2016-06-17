package client

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"log"
)

type Client struct {
	rootCmd *cobra.Command
	dialer  *Dialer
}

func (c *Client) Exec() error {
	return c.rootCmd.Execute()
}

func NewClient(dialer *Dialer) (*Client, error) {
	r := &Client{nil, dialer}

	rootCmd := &cobra.Command{
		Use:   "scootapi",
		Short: "scootapi is a command-line client to Cloud Scoot",
		Run:   func(*cobra.Command, []string) {},
	}

	r.rootCmd = rootCmd

	rootCmd.AddCommand(makeRunJobCmd(r))
	return r, nil
}

func NewDialer(addr string, transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) *Dialer {
	return &Dialer{addr, transportFactory, protocolFactory, nil}
}

type Dialer struct {
	addr             string
	transportFactory thrift.TTransportFactory
	protocolFactory  thrift.TProtocolFactory
	client           *scoot.ProcClient
}

func (d *Dialer) Dial() (*scoot.ProcClient, error) {
	if d.client == nil {
		log.Println("Dialing", d.addr)
		var transport thrift.TTransport
		transport, err := thrift.NewTSocket(d.addr)
		if err != nil {
			return nil, fmt.Errorf("Error opening socket: %v", err)
		}
		transport = d.transportFactory.GetTransport(transport)
		err = transport.Open()
		if err != nil {
			return nil, fmt.Errorf("Error opening transport: %v", err)
		}
		d.client = scoot.NewProcClientFactory(transport, d.protocolFactory)
	}
	return d.client, nil
}

func (d *Dialer) Close() error {
	if d.client != nil {
		return d.client.Transport.Close()
	}
	return nil
}
