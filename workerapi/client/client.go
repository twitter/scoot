package client

import (
	"fmt"

	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/workerapi"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

const defaultWorkerAddr = "localhost:9090"

// Interface for
type Client interface {
	// Connection funtions
	Dial() error
	Close() error

	// Worker API Interactions
	QueryWorker() (workerapi.WorkerStatus, error)
	runner.Controller
	runner.LegacyStatuses
}

// Basic implementation of Client interface for interaction with Scoot worker API
type simpleClient struct {
	addr         string
	dialer       dialer.Dialer
	workerClient *worker.WorkerClient
}

func NewSimpleClient(di dialer.Dialer, addr string) (Client, error) {
	cl := &simpleClient{}
	cl.dialer = di
	cl.addr = addr
	return cl, nil
}

func (c *simpleClient) Dial() error {
	_, err := c.dial()
	return err
}

func (c *simpleClient) dial() (*worker.WorkerClient, error) {
	if c.workerClient == nil {
		if c.addr == "" {
			c.addr = defaultWorkerAddr
		}

		transport, protocolFactory, err := c.dialer.Dial(c.addr)
		if err != nil {
			return nil, fmt.Errorf("Error dialing to set up client connection: %v", err)
		}

		c.workerClient = worker.NewWorkerClientFactory(transport, protocolFactory)
	}
	return c.workerClient, nil
}

func (c *simpleClient) Close() error {
	if c.workerClient != nil {
		return c.workerClient.Transport.Close()
	}
	return nil
}

func (c *simpleClient) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	workerClient, err := c.dial()
	if err != nil {
		return runner.ProcessStatus{}, err
	}

	status, err := workerClient.Run(workerapi.DomainRunCommandToThrift(cmd))
	if err != nil {
		return runner.ProcessStatus{}, err
	}
	return workerapi.ThriftRunStatusToDomain(status), nil
}

func (c *simpleClient) Abort(runId runner.RunId) (runner.ProcessStatus, error) {
	workerClient, err := c.dial()
	if err != nil {
		return runner.ProcessStatus{}, err
	}

	status, err := workerClient.Abort(string(runId))
	if err != nil {
		return runner.ProcessStatus{}, err
	}
	return workerapi.ThriftRunStatusToDomain(status), nil
}

func (c *simpleClient) QueryWorker() (workerapi.WorkerStatus, error) {
	workerClient, err := c.dial()
	if err != nil {
		return workerapi.WorkerStatus{}, err
	}

	status, err := workerClient.QueryWorker()
	if err != nil {
		return workerapi.WorkerStatus{}, err
	}
	return workerapi.ThriftWorkerStatusToDomain(status), nil
}

func (c *simpleClient) Status(id runner.RunId) (runner.ProcessStatus, error) {
	st, err := c.QueryWorker()
	if err != nil {
		return runner.ProcessStatus{}, err
	}
	for _, p := range st.Runs {
		if p.RunId == id {
			return p, nil
		}
	}
	return runner.ProcessStatus{}, fmt.Errorf("no such process %v", id)
}

func (c *simpleClient) StatusAll() ([]runner.ProcessStatus, error) {
	st, err := c.QueryWorker()
	if err != nil {
		return nil, err
	}
	return st.Runs, nil
}

//TODO: implement erase
func (c *simpleClient) Erase(run runner.RunId) error {
	panic(fmt.Errorf("workerapi/client:Erase not yet implemented"))
}
