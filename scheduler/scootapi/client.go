package scootapi

// scootapi provides client side access to the scoot services.  It is used by the
// command line binaries to submit (thrift) requests to the scoot services.

import (
	"fmt"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/scheduler/scootapi/gen-go/scoot"
)

// A struct that supports establishing and maintaining a
// connection for making requests to a CloudScootClient.
// This client can only serve one request at a time.
type CloudScootClient struct {
	addr   string
	dialer dialer.Dialer
	client *scoot.CloudScootClient
}

// Parameters to configure a CloudScootClient connection
type CloudScootClientConfig struct {
	Addr   string        //Address to connect to
	Dialer dialer.Dialer // dialer to use to connect to address
}

// Creates a NewCloudScootClient.  Returns a client object which can
// be used to execute calls to Scoot Cloud Exec.
func NewCloudScootClient(config CloudScootClientConfig) *CloudScootClient {
	return &CloudScootClient{
		addr:   config.Addr,
		dialer: config.Dialer,
		client: nil,
	}
}

// RunJob API. Schedules a Job to run asynchronously via CloudExecScoot based on
//the specified job. If successful the JobId is returned if not an error.
func (c *CloudScootClient) RunJob(jobDef *scoot.JobDefinition) (r *scoot.JobId, err error) {
	err = c.checkForClient()
	if err != nil {
		return nil, err
	}
	jobId, err := c.client.RunJob(jobDef)
	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		// this could cause an error when closing transport
		// but we don't care do our best effort and move on
		c.closeConnection()
	}
	return jobId, err
}

// GetStatus API. Returns the JobStatus of the specified JobId if successful,
// otherwise an erorr.
func (c *CloudScootClient) GetStatus(jobId string) (r *scoot.JobStatus, err error) {
	err = c.checkForClient()
	if err != nil {
		return nil, err
	}
	jobStatus, err := c.client.GetStatus(jobId)
	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		// this could cause an error when closing transport
		// but we don't care do our best effort and move on
		c.closeConnection()
	}
	return jobStatus, err
}

// Close any open Transport associated with this ScootClient
func (c *CloudScootClient) Close() error {
	if c.client != nil {
		return c.closeConnection()
	}

	return nil
}

func (c *CloudScootClient) KillJob(jobId string) (r *scoot.JobStatus, err error) {
	err = c.checkForClient()
	if err != nil {
		return nil, err
	}
	jobStatus, err := c.client.KillJob(jobId)
	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		// this could cause an error when closing transport
		// but we don't care do our best effort and move on
		c.closeConnection()
	}
	return jobStatus, err
}

func (c *CloudScootClient) OfflineWorker(req *scoot.OfflineWorkerReq) error {
	err := c.checkForClient()
	if err != nil {
		return err
	}
	err = c.client.OfflineWorker(req)
	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		// this could cause an error when closing transport
		// but we don't care do our best effort and move on
		c.closeConnection()
	}
	return err
}

func (c *CloudScootClient) ReinstateWorker(req *scoot.ReinstateWorkerReq) error {
	err := c.checkForClient()
	if err != nil {
		return err
	}
	err = c.client.ReinstateWorker(req)
	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		// this could cause an error when closing transport
		// but we don't care do our best effort and move on
		c.closeConnection()
	}
	return err
}

func (c *CloudScootClient) SetSchedulerStatus(maxTasks int32) error {
	// validation is also implemented in sched/definitions.go.  We cannot use it here because it
	// causes a circular dependency.  The two implementations can be consolidated when the code
	// is restructured
	if maxTasks < -1 {
		return fmt.Errorf("invlid max tasks value:%d. Must be >= -1.", maxTasks)
	}

	err := c.checkForClient()
	if err != nil {
		return err
	}

	err = c.client.SetSchedulerStatus(maxTasks)
	return err
}

func (c *CloudScootClient) GetSchedulerStatus() (*scoot.SchedulerStatus, error) {
	err := c.checkForClient()
	if err != nil {
		return nil, err
	}
	schedulerStatus, err := c.client.GetSchedulerStatus()
	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		// this could cause an error when closing transport
		// but we don't care do our best effort and move on
		c.closeConnection()
	}
	return schedulerStatus, err
}

// helper method to check for a non-nil client / create one
func (c *CloudScootClient) checkForClient() (err error) {
	if c.client == nil {
		c.client, err = createClient(c.addr, c.dialer)
		if err != nil {
			return err
		}
	}
	return nil
}

// helper method to create a scoot.CloudScootClient
func createClient(addr string, dialer dialer.Dialer) (*scoot.CloudScootClient, error) {
	transport, protocolFactory, err := dialer.Dial(addr)
	if err != nil {
		return nil, fmt.Errorf("Error dialing to set up client connection: %v", err)
	}

	return scoot.NewCloudScootClientFactory(transport, protocolFactory), nil
}

// helper method to close the connection and reset the
// struct field to nil so it will get recreated next
func (c *CloudScootClient) closeConnection() error {
	err := c.client.Transport.Close()
	c.client = nil

	return err
}
