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

	client, err := client.NewClient(transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Cannot initialize Worker CL: ", err)
	}
	err = client.Exec()
	if err != nil {
		log.Fatal("error running workercl ", err)
	}
}
