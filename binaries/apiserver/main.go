package main

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/scootapi/server"
	"log"
)

func main() {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

	// TODO: upgrade to durable queue
	queue := memory.NewSimpleQueue(1)

	//TODO: replace with durable sagaLog
	sagaCoord := saga.MakeInMemorySagaCoordinator()

	handler := server.NewHandler(queue, sagaCoord)

	// TODO: read from a config
	addr := "localhost:9090"
	err := server.Serve(handler, addr, transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error serving Scoot API: ", err)
	}
}
