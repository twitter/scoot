package memory

// In-memory Scheduler Queue

import (
	"fmt"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
	"time"
)

func NewSimpleQueue() (queue.Queue, chan sched.Job) {
	q := &simpleQueue{}
	q.outCh = make(chan sched.Job, 1)
	q.inCh = make(chan enqueueReq)
	go q.loop()
	return q, q.outCh
}

type simpleQueue struct {
	nextID int
	inCh   chan enqueueReq
	outCh  chan sched.Job
}

type enqueueReq struct {
	job      sched.Job
	resultCh chan enqueueResult
}

type enqueueResult struct {
	jobID string
	err   error
}

func (q *simpleQueue) Enqueue(job sched.Job) (string, error) {
	if job.Id != "" {
		return "", queue.NewInvalidJobRequest(fmt.Sprintf("job.Id must be unset; was %v", job.Id))
	}
	if len(job.Tasks) == 0 {
		return "", queue.NewInvalidJobRequest("job must have at least 1 task")
	}
	for _, task := range job.Tasks {
		if task.Id == "" {
			return "", queue.NewInvalidJobRequest("task.Id must be set")
		}
		if len(task.Command) == 0 {
			return "", queue.NewInvalidJobRequest("task.Commands must be set")
		}
	}
	resultCh := make(chan enqueueResult)
	q.inCh <- enqueueReq{job, resultCh}
	result := <-resultCh
	return result.jobID, result.err
}

func (q *simpleQueue) Close() error {
	return nil
}

func (q *simpleQueue) loop() {
	for {
		req, ok := <-q.inCh
		if !ok {
			// Channel is cloed
			return
		}
		job := req.job
		id := fmt.Sprintf("%v", q.nextID)
		q.nextID++
		job.Id = id
		select {
		case q.outCh <- job:
			req.resultCh <- enqueueResult{id, nil}
		default:
			req.resultCh <- enqueueResult{"", queue.NewCanNotScheduleNow(1*time.Second, "queue full")}
		}
	}
}
