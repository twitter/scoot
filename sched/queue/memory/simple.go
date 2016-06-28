package memory

// In-memory Scheduler Queue

import (
	"fmt"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
	"time"
)

func NewSimpleQueue() (queue.Queue, chan queue.WorkItem) {
	q := &simpleQueue{}
	q.outCh = make(chan queue.WorkItem, 1)
	q.inCh = make(chan enqueueReq)
	go q.loop()
	return q, q.outCh
}

type simpleQueue struct {
	nextID int
	inCh   chan enqueueReq
	outCh  chan queue.WorkItem
}

type simpleWorkItem sched.Job

func (i simpleWorkItem) Job() sched.Job {
	return sched.Job(i)
}

func (i simpleWorkItem) Dequeue() {
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
	err := queue.ValidateJob(job)
	if err != nil {
		return "", err
	}
	resultCh := make(chan enqueueResult)
	q.inCh <- enqueueReq{job, resultCh}
	result := <-resultCh
	return result.jobID, result.err
}

func (q *simpleQueue) Close() error {
	close(q.inCh)
	close(q.outCh)
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
		case q.outCh <- simpleWorkItem(job):
			req.resultCh <- enqueueResult{id, nil}
		default:
			req.resultCh <- enqueueResult{"", queue.NewCanNotScheduleNow(1*time.Second, "queue full")}
		}
	}
}
