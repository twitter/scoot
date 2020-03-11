package thrift

import (
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/twitter/scoot/scheduler/server"
)

/**
Kill the job identified by jobId.  Update the saga for the job to indicate
that it was killed.  Return a job status indicating that the job was successfully
killed or one of the following errors
*/
func KillJob(jobId string, scheduler server.Scheduler, sc saga.SagaCoordinator) (*scoot.JobStatus, error) {
	err := scheduler.KillJob(jobId)
	if err != nil {
		return nil, err
	}

	return GetJobStatus(jobId, sc)
}
