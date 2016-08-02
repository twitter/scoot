package main

import (
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/workerapi/client"
)

// Binary to talk to Cloud Worker
func main() {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

	client := client.NewCliClient(transportFactory, protocolFactory)
	err := client.Cli()
	if err != nil {
		log.Fatal("error running workercl ", err)
	}
}
