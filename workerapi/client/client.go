// The client package provides an interface and implementation for the
// Scoot Worker API, as well as a CLI client that wraps it.
package client

import (
	"fmt"

	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/workerapi"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

const defaultWorkerAddr = "localhost:9090"

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
func (c *simpleClient) Status(id runner.RunID) (runner.RunStatus, error) {
	st, err := c.QueryWorker()
	if err != nil {
		return runner.RunStatus{}, err
	}
	for _, p := range st.Runs {
		if p.RunID == id {
			return p, nil
		}
	}
	return runner.RunStatus{}, fmt.Errorf("no such process %v", id)
}

// Implements Scoot Worker API
func (c *simpleClient) StatusAll() ([]runner.RunStatus, error) {
	st, err := c.QueryWorker()
	if err != nil {
		return nil, err
	}
	return st.Runs, nil
}

func (c *simpleClient) QueryNow(q runner.Query) ([]runner.RunStatus, error) {
	stats, err := c.StatusAll()
	if err != nil {
		return nil, err
	}
	return runners.StatusesRO(stats).QueryNow(q)
}

//TODO: implement erase
func (c *simpleClient) Erase(run runner.RunID) error {
	panic(fmt.Errorf("workerapi/client:Erase not yet implemented"))
}
