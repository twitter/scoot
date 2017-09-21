package server

import (
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched/scheduler"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
)

/**
Kill the job identified by jobId.  Update the saga for the job to indicate
that it was killed.  Return a job status indicating that the job was successfully
killed or one of the following errors
*/
func KillJob(jobId string, scheduler scheduler.Scheduler, sc saga.SagaCoordinator) (*scoot.JobStatus, error) {

	err := scheduler.KillJob(jobId)
	if err != nil {
		return nil, err
	}

	return GetJobStatus(jobId, sc)

}
