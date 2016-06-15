package main

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/scootdev/scoot/scootapi"
	"log"
)

func main() {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

	err := scootapi.Serve(scootapi.NewHandler(), "localhost:9090", transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error serving Scoot API: ", err)
	}
}
