package main

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/scootapi/server"
	"log"
)

func main() {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

	// TODO: upgrade to durable queue
	queue, _ := memory.NewSimpleQueue(1)
	handler := server.NewHandler(queue)

	// TODO: read from a config
	addr := "localhost:9090"
	err := server.Serve(handler, addr, transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error serving Scoot API: ", err)
	}
}
