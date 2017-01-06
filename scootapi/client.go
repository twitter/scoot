package scootapi

import (
	"fmt"
	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
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

// helper method to create a scoot.CloudScootClient
func createClient(addr string, dialer dialer.Dialer) (*scoot.CloudScootClient, error) {
	transport, protocolFactory, err := dialer.Dial(addr)
	if err != nil {
		return nil, fmt.Errorf("Error dialing to set up client connection: %v", err)
	}

	return scoot.NewCloudScootClientFactory(transport, protocolFactory), nil
}

// helper method to reset the connection
func (c *CloudScootClient) resetConnection() {
	c.client.Transport.Close()
	c.client = nil
}

// RunJob API. Schedules a Job to run asynchronously via CloudExecScoot based on
//the specified job. If successful the JobId is returned if not an error.
func (c *CloudScootClient) RunJob(jobDef *scoot.JobDefinition) (r *scoot.JobId, err error) {
	if c.client == nil {
		c.client, err = createClient(c.addr, c.dialer)
		if err != nil {
			return nil, err
		}
	}

	jobId, err := c.client.RunJob(jobDef)

	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		c.resetConnection()
	}

	return jobId, err
}

// GetStatus API. Returns the JobStatus of the specified JobId if successful,
// otherwise an erorr.
func (c *CloudScootClient) GetStatus(jobId string) (r *scoot.JobStatus, err error) {
	if c.client == nil {
		c.client, err = createClient(c.addr, c.dialer)
		if err != nil {
			return nil, err
		}
	}

	jobStatus, err := c.client.GetStatus(jobId)

	// if an error occurred reset the connection, could be a broken pipe or other
	// unrecoverable error.  reset connection so a new clean one gets created
	// on the next request
	if err != nil {
		c.resetConnection()
	}

	return jobStatus, err
}

// Close any open Transport associated with this ScootClient
func (c *CloudScootClient) Close() error {
	if c.client != nil {
		return c.client.Transport.Close()
	}

	return nil
}
