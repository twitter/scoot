package main

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/scootapi/client"
	"log"
)

// Binary to talk to Cloud Scoot API
func main() {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

	dialer := client.NewDialer("localhost:9090", transportFactory, protocolFactory)
	client, err := client.NewClient(dialer)
	if err != nil {
		log.Fatal("Cannot initialize Cloud Scoot CLI: ", err)
	}
	err = client.Exec()
	if err != nil {
		log.Fatal("error running scootapi ", err)
	}
}
