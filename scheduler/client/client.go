package client

// client provides client side access to the scoot services.  It is used by the
// command line binaries to submit (thrift) requests to the scoot services.

import (
	"fmt"

	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
)

// CloudScootClient A struct that supports establishing and maintaining a
// connection for making requests to a CloudScootClient.
// This client can only serve one request at a time.
type CloudScootClient struct {
	addr   string
	dialer dialer.Dialer
	client *scoot.CloudScootClient
}

// CloudScootClientConfig Parameters to configure a CloudScootClient connection
type CloudScootClientConfig struct {
	Addr   string        //Address to connect to
	Dialer dialer.Dialer // dialer to use to connect to address
}

// NewCloudScootClient Creates a CloudScootClient.  Returns a client object which can
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
	if err := c.checkForClient(); err != nil {
		return nil, err
	}
	jobID, err := c.client.RunJob(jobDef)
	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		// this could cause an error when closing transport
		// but we don't care do our best effort and move on
		c.closeConnection()
	}
	return jobID, err
}

// GetStatus API. Returns the JobStatus of the specified JobId if successful,
// otherwise an erorr.
func (c *CloudScootClient) GetStatus(jobID string) (r *scoot.JobStatus, err error) {
	if err := c.checkForClient(); err != nil {
		return nil, err
	}
	jobStatus, err := c.client.GetStatus(jobID)
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
	return c.checkForClient()
}

// KillJob kill a job
func (c *CloudScootClient) KillJob(jobID string) (r *scoot.JobStatus, err error) {
	if err := c.checkForClient(); err != nil {
		return nil, err
	}
	jobStatus, err := c.client.KillJob(jobID)
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

// OfflineWorker offline a worker (disable scheduler from using it)
func (c *CloudScootClient) OfflineWorker(req *scoot.OfflineWorkerReq) error {
	if err := c.checkForClient(); err != nil {
		return err
	}
	err := c.client.OfflineWorker(req)
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

// ReinstateWorker reinstate a worker's availability to the scheduler
func (c *CloudScootClient) ReinstateWorker(req *scoot.ReinstateWorkerReq) error {
	if err := c.checkForClient(); err != nil {
		return err
	}
	err := c.client.ReinstateWorker(req)
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

// SetSchedulerStatus set the max tasks we want to have submitted to the scheduler
func (c *CloudScootClient) SetSchedulerStatus(maxTasks int32) error {
	// validation is also implemented in sched/definitions.go.  We cannot use it here because it
	// causes a circular dependency.  The two implementations can be consolidated when the code
	// is restructured
	if maxTasks < -1 {
		return fmt.Errorf("invlid max tasks value:%d. Must be >= -1", maxTasks)
	}

	if err := c.checkForClient(); err != nil {
		return err
	}

	return c.client.SetSchedulerStatus(maxTasks)
}

// GetSchedulerStatus get the scheduler's max task setting
func (c *CloudScootClient) GetSchedulerStatus() (*scoot.SchedulerStatus, error) {
	if err := c.checkForClient(); err != nil {
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

// GetClassLoadPcts get the target load pcts for the classes
func (c *CloudScootClient) GetClassLoadPcts() (map[string]int32, error) {
	if err := c.checkForClient(); err != nil {
		return nil, err
	}
	classLoadPcts, err := c.client.GetClassLoadPcts()
	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		// this could cause an error when closing transport
		// but we don't care do our best effort and move on
		c.closeConnection()
	}
	return classLoadPcts, err
}

// SetClassLoadPcts set the target worker load % for each job class
func (c *CloudScootClient) SetClassLoadPcts(classLoads map[string]int32) error {
	if err := c.checkForClient(); err != nil {
		return err
	}

	return c.client.SetClassLoadPcts(classLoads)
}

// GetRequestorToClassMap get map of requestor (reg exp) to class load pct
func (c *CloudScootClient) GetRequestorToClassMap() (map[string]string, error) {
	if err := c.checkForClient(); err != nil {
		return nil, err
	}
	requestorToClassMap, err := c.client.GetRequestorToClassMap()
	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		// this could cause an error when closing transport
		// but we don't care do our best effort and move on
		c.closeConnection()
	}
	return requestorToClassMap, err
}

// SetRequestorToClassMap set the map of requestor (requestor value is reg exp) to class name
func (c *CloudScootClient) SetRequestorToClassMap(requestorToClassMap map[string]string) error {
	if err := c.checkForClient(); err != nil {
		return err
	}

	return c.client.SetRequestorToClassMap(requestorToClassMap)
}

// GetRebalanceMinDuration get the minimum time the scheduler load % must we over the re-balance
// threshold before re-balancing
func (c *CloudScootClient) GetRebalanceMinDuration() (int32, error) {
	if err := c.checkForClient(); err != nil {
		return -1, err
	}

	return c.client.GetRebalanceMinDuration()
}

// SetRebalanceMinDuration set the minimum time the scheduler load % must we over the re-balance
// threshold before re-balancing
func (c *CloudScootClient) SetRebalanceMinDuration(durationMin int32) error {
	if err := c.checkForClient(); err != nil {
		return err
	}

	return c.client.SetRebalanceMinDuration(durationMin)
}

// GetRebalanceThreshold get the minimum difference between under/over allocated %s that will trigger
// re-balancing
func (c *CloudScootClient) GetRebalanceThreshold() (int32, error) {
	if err := c.checkForClient(); err != nil {
		return -1, err
	}

	return c.client.GetRebalanceThreshold()
}

// SetRebalanceThreshold set the minimum difference between under/over allocated %s that will trigger
// re-balancing
func (c *CloudScootClient) SetRebalanceThreshold(threshold int32) error {
	if err := c.checkForClient(); err != nil {
		return err
	}

	return c.client.SetRebalanceThreshold(threshold)
}

// helper method to check for a non-nil client / create one
func (c *CloudScootClient) checkForClient() (err error) {
	if c.client == nil {
		c.client, err = createClient(c.addr, c.dialer)
		return err
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
