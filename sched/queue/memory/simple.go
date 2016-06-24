package memory

// In-memory Scheduler Queue

import (
	"github.com/scootdev/scoot/sched"
)

type simpleQueue struct {
	nextID  int
	nextJob *sched.Job
	readCh  chan sched.Job
	inCh    chan enqueueReq
}

type enqueueReq struct {
	job      sched.Job
	resultCh chan enqueueResult
}

type enqueueResult struct {
	jobID sched.JobID
	err   error
}

func (q *simpleQueue) Enqueue(job sched.Job) (sched.JobID, error) {
	return sched.JobID(""), fmt.Errorf("Not yet implemented")
}

func (q *simpleQueue) Read() (chan sched.Job, error) {
	return nil, fmt.Errorf("Not yet implemented")
}

func (q *simpleQueue) Close() error {
	return nil
}
