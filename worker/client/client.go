// The client package provides an interface and implementation for the
// Scoot Worker API, as well as a CLI client that wraps it.
package client

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/wisechengyi/scoot/common/dialer"
	"github.com/wisechengyi/scoot/runner"
	"github.com/wisechengyi/scoot/runner/runners"
	"github.com/wisechengyi/scoot/worker/domain"
	"github.com/wisechengyi/scoot/worker/domain/gen-go/worker"
)

// Parameters for configuring connections to remote workers.
type WorkersClientJSONConfig struct {
	Type          string // transport type: rpc
	PollingPeriod string // polling period default to 250ms
}

type Client interface {
	// Connection funtions
	Dial() error
	Close() error

	// Worker API Interactions
	QueryWorker() (domain.WorkerStatus, error)
	runner.Controller
	runner.StatusQueryNower
	runner.LegacyStatusReader
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
			c.addr = domain.DefaultWorker_Thrift
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

	status, err := workerClient.Run(domain.DomainRunCommandToThrift(cmd))
	if err != nil {
		return runner.RunStatus{}, err
	}
	return domain.ThriftRunStatusToDomain(status), nil
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
	return domain.ThriftRunStatusToDomain(status), nil
}

// Release local resources.
func (c *simpleClient) Release() {
	c.Close()
}

// Implements Scoot Worker API
func (c *simpleClient) QueryWorker() (domain.WorkerStatus, error) {
	workerClient, err := c.dial()
	if err != nil {
		return domain.WorkerStatus{}, err
	}

	status, err := workerClient.QueryWorker()
	if err != nil {
		return domain.WorkerStatus{}, err
	}
	return domain.ThriftWorkerStatusToDomain(status), nil
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
	svc := runner.ServiceStatus{Initialized: ws.Initialized, IsHealthy: ws.IsHealthy, Error: svcErr}
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
	return ws.Runs, runner.ServiceStatus{Initialized: ws.Initialized, IsHealthy: ws.IsHealthy, Error: svcErr}, nil
}

func (c *simpleClient) QueryNow(q runner.Query) ([]runner.RunStatus, runner.ServiceStatus, error) {
	st, svc, err := c.StatusAll()
	if err != nil {
		return nil, runner.ServiceStatus{}, err
	}
	st, err = runners.StatusesRO(st).QueryNow(q)
	return st, svc, err
}

// Create a Bundlestore URI from an addr
func APIAddrToBundlestoreURI(addr string) string {
	return "http://" + addr + "/bundle/"
}

// Get the path of the file containing the address for scootapi to use
func GetScootapiAddrPath() string {
	optionalId := os.Getenv("SCOOT_ID") // Used to connect to a different set of scoot processes.
	return path.Join(os.Getenv("HOME"), ".cloudscootaddr"+optionalId)
}

// Get the scootapi address (as host:port)
func GetScootapiAddr() (sched string, api string, err error) {
	data, err := ioutil.ReadFile(GetScootapiAddrPath())
	if err != nil {
		if os.IsNotExist(err) {
			return "", "", nil
		}
		return "", "", err
	}
	addrs := strings.Split(string(data), "\n")
	if len(addrs) != 2 {
		return "", "", errors.New("Expected both sched and api addrs, got: " + string(data))
	}
	return string(addrs[0]), string(addrs[1]), nil
}
