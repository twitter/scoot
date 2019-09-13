// The client package provides an interface and implementation for the
// Scoot Worker API, as well as a CLI client that wraps it.
package client

import (
	"errors"
	"fmt"

	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/scootapi"
	"github.com/twitter/scoot/workerapi"
	"github.com/twitter/scoot/workerapi/gen-go/worker"
)

type Client interface {
	// Connection funtions
	Dial() error
	Close() error

	// Worker API Interactions
	QueryWorker() (workerapi.WorkerStatus, error)
	runner.Controller
	runner.StatusQueryNower
	runner.LegacyStatusReader
	runner.StatusEraser
}

type simpleClient struct {
	addr         string
	dialer       dialer.Dialer
	workerClient *worker.WorkerClient
}

// Create basic implementation of Client interface for interaction with Scoot worker API
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
			c.addr = scootapi.DefaultWorker_Thrift
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

// Implements Scoot Worker API
func (c *simpleClient) Run(cmd *runner.Command) (runner.RunStatus, error) {
	workerClient, err := c.dial()
	if err != nil {
		return runner.RunStatus{}, err
	}

	status, err := workerClient.Run(workerapi.DomainRunCommandToThrift(cmd))
	if err != nil {
		return runner.RunStatus{}, err
	}
	return workerapi.ThriftRunStatusToDomain(status), nil
}

// Implements Scoot Worker API
func (c *simpleClient) Abort(runID runner.RunID) (runner.RunStatus, error) {
	workerClient, err := c.dial()
	if err != nil {
		return runner.RunStatus{}, err
	}

	status, err := workerClient.Abort(string(runID))
	if err != nil {
		return runner.RunStatus{}, err
	}
	return workerapi.ThriftRunStatusToDomain(status), nil
}

// Release local resources.
func (c *simpleClient) Release() {
	c.Close()
}

// Implements Scoot Worker API
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

// Implements Scoot Worker API
func (c *simpleClient) Status(id runner.RunID) (runner.RunStatus, runner.ServiceStatus, error) {
	ws, err := c.QueryWorker()
	if err != nil {
		return runner.RunStatus{}, runner.ServiceStatus{}, err
	}
	var svcErr error
	if ws.Error != "" {
		svcErr = errors.New(ws.Error)
	}
	svc := runner.ServiceStatus{Initialized: ws.Initialized, Error: svcErr}
	for _, p := range ws.Runs {
		if p.RunID == id {
			return p, svc, nil
		}
	}
	return runner.RunStatus{}, svc, fmt.Errorf("no such process %v", id)
}

// Implements Scoot Worker API
func (c *simpleClient) StatusAll() ([]runner.RunStatus, runner.ServiceStatus, error) {
	ws, err := c.QueryWorker()
	if err != nil {
		return nil, runner.ServiceStatus{}, err
	}
	var svcErr error
	if ws.Error != "" {
		svcErr = errors.New(ws.Error)
	}
	return ws.Runs, runner.ServiceStatus{Initialized: ws.Initialized, Error: svcErr}, nil
}

func (c *simpleClient) QueryNow(q runner.Query) ([]runner.RunStatus, runner.ServiceStatus, error) {
	st, svc, err := c.StatusAll()
	if err != nil {
		return nil, runner.ServiceStatus{}, err
	}
	st, err = runners.StatusesRO(st).QueryNow(q)
	return st, svc, err
}

//TODO: implement erase
func (c *simpleClient) Erase(run runner.RunID) error {
	panic(fmt.Errorf("workerapi/client:Erase not yet implemented"))
}
