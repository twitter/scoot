package client

import (
	"fmt"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
)

func (c *Client) Dial() (*scoot.CloudScootClient, error) {
	if c.client == nil {
		client, err := c.dialer.Dial(c.addr)
		if err != nil {
			return nil, err
		}
		c.client = client
	}
	return c.client, nil
}

func (c *Client) Close(cmd *cobra.Command, args []string) error {
	if c.client != nil {
		return c.client.Transport.Close()
	}
	return nil
}

type Dialer interface {
	// Returns a CloudScoot Client with open Transport, or an error
	Dial(addr string) (*scoot.CloudScootClient, error)
}

func NewDialer(tf thrift.TTransportFactory, pf thrift.TProtocolFactory) Dialer {
	return &dialer{tf, pf}
}

type dialer struct {
	transportFactory thrift.TTransportFactory
	protocolFactory  thrift.TProtocolFactory
}

func (d *dialer) Dial(addr string) (*scoot.CloudScootClient, error) {
	if addr == "" {
		addr = scootapi.GetScootapiAddr()
		if addr == "" {
			addr = "localhost:9090"
		}
	}
	log.Println("Dialing", addr)
	var transport thrift.TTransport
	transport, err := thrift.NewTSocket(addr)
	if err != nil {
		return nil, fmt.Errorf("Error opening socket: %v", err)
	}
	transport = d.transportFactory.GetTransport(transport)
	err = transport.Open()
	if err != nil {
		return nil, fmt.Errorf("Error opening transport: %v", err)
	}
	return scoot.NewCloudScootClientFactory(transport, d.protocolFactory), nil
}
