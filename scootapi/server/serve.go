package server

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"log"
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

type Handler struct{}

func NewHandler() scoot.Proc {
	return &Handler{}
}

func (h *Handler) RunJob(def *scoot.JobDefinition) (*scoot.Job, error) {
	log.Println("Running job %v", def)
	r := scoot.NewInvalidRequest()
	msg := "Scoot is working by saying it won't work"
	r.Message = &msg
	return nil, r
}
