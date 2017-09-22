// Library for establishing Thrift network connections for clients.
// Provides Dialer interface with basic implementation.
package dialer

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/apache/thrift/lib/go/thrift"
)

// Interface for initializing a thrift connection for a client
type Dialer interface {
	Dial(addr string) (thrift.TTransport, thrift.TProtocolFactory, error)
}

type simpleDialer struct {
	transportFactory thrift.TTransportFactory
	protocolFactory  thrift.TProtocolFactory
	timeout          time.Duration
}

// Create instance of basic Dialer that manages thrift transport/protocol factories.
// Opens a thrift connection directly to the given address.
func NewSimpleDialer(tf thrift.TTransportFactory, pf thrift.TProtocolFactory, timeout time.Duration) Dialer {
	return &simpleDialer{transportFactory: tf, protocolFactory: pf, timeout: timeout}
}

func (d *simpleDialer) Dial(addr string) (thrift.TTransport, thrift.TProtocolFactory, error) {
	log.Info("Dialing", addr)

	var transport thrift.TTransport
	transport, err := thrift.NewTSocketTimeout(addr, d.timeout)
	if err != nil {
		return nil, nil, fmt.Errorf("Error opening socket: %v", err)
	}

	transport = d.transportFactory.GetTransport(transport)
	err = transport.Open()
	if err != nil {
		return nil, nil, fmt.Errorf("Error opening transport: %v", err)
	}

	return transport, d.protocolFactory, nil
}
