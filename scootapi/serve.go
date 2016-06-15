package scootapi

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

func Serve(handler scoot.Proc, addr string, transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) error {
	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		return err
	}
	processor := scoot.NewProcProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	fmt.Println("About to serve")

	return server.Serve()
}
