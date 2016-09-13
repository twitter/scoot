package client

import (
	"fmt"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/workerapi"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

type Client interface {
	Dial() error
	Close() error
	Run(*runner.Command) (*runner.ProcessStatus, error)
	Abort(runId string) (*runner.ProcessStatus, error)
	QueryWorker() (*workerapi.WorkerStatus, error)
}

type client struct {
	addr             string
	transportFactory thrift.TTransportFactory
	protocolFactory  thrift.TProtocolFactory
	worker           *worker.WorkerClient
}

func NewClient(
	transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string,
) Client {
	r := &client{}
	r.transportFactory = transportFactory
	r.protocolFactory = protocolFactory
	r.addr = addr
	return r
}

func (c *client) Dial() error {
	_, err := c.dial()
	return err
}

func (c *client) dial() (*worker.WorkerClient, error) {
	if c.worker == nil {
		if c.addr == "" {
			return nil, fmt.Errorf("Cannot dial: no address")
		}
		log.Println("Dialing", c.addr)
		var transport thrift.TTransport
		transport, err := thrift.NewTSocket(c.addr)
		if err != nil {
			return nil, fmt.Errorf("Error opening socket: %v", err)
		}
		transport = c.transportFactory.GetTransport(transport)
		err = transport.Open()
		if err != nil {
			return nil, fmt.Errorf("Error opening transport: %v", err)
		}
		c.worker = worker.NewWorkerClientFactory(transport, c.protocolFactory)
	}
	return c.worker, nil
}

func (c *client) Close() error {
	if c.worker != nil {
		return c.worker.Transport.Close()
	}
	return nil
}

func (c *client) Run(cmd *runner.Command) (*runner.ProcessStatus, error) {
	client, err := c.dial()
	if err != nil {
		return &runner.ProcessStatus{}, err
	}

	status, err := client.Run(workerapi.DomainRunCommandToThrift(cmd))
	if err != nil {
		return &runner.ProcessStatus{}, err
	}
	return workerapi.ThriftRunStatusToDomain(status), nil
}

func (c *client) Abort(runId string) (*runner.ProcessStatus, error) {
	client, err := c.dial()
	if err != nil {
		return &runner.ProcessStatus{}, err
	}

	status, err := client.Abort(runId)
	if err != nil {
		return &runner.ProcessStatus{}, err
	}
	return workerapi.ThriftRunStatusToDomain(status), nil
}

func (c *client) QueryWorker() (*workerapi.WorkerStatus, error) {
	client, err := c.dial()
	if err != nil {
		return &workerapi.WorkerStatus{}, err
	}

	status, err := client.QueryWorker()
	if err != nil {
		return &workerapi.WorkerStatus{}, err
	}
	return workerapi.ThriftWorkerStatusToDomain(status), nil
}

//TODO: implement erase
