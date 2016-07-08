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

	queue, _ := memory.NewSimpleQueue()
	handler := server.NewHandler(queue)
	err := server.Serve(handler, "localhost:9090", transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error serving Scoot API: ", err)
	}
}
