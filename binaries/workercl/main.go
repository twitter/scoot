package main

import (
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/workerapi/client"
)

// Binary to talk to Cloud Worker
func main() {
	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	dialer := client.NewSimpleDialer(transportFactory, protocolFactory)
	client, err := client.NewSimpleCLIClient(dialer)
	if err != nil {
		log.Fatal("Failed to create worker CLIClient: ", err)
	}

	err = client.Exec()
	if err != nil {
		log.Fatal("Error running workercl: ", err)
	}
}
