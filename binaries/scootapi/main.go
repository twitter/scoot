package main

import (
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/scootapi/client"
)

// Binary to talk to Cloud Scoot API
func main() {
	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	d := dialer.NewSimpleDialer(transportFactory, protocolFactory)
	client, err := client.NewSimpleCLIClient(d)
	if err != nil {
		log.Fatal("Failed to create new ScootAPI CLI client: ", err)
	}

	err = client.Exec()
	if err != nil {
		log.Fatal("Error running scootapi ", err)
	}
}
