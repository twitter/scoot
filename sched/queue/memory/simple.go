package memory

// In-memory Scheduler Queue

import (
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
	"strconv"
	"time"
)

func NewSimpleQueue(capacity int) queue.Queue {
	q := &simpleQueue{}
	q.reqCh = make(chan queueReq)
	q.outCh = make(chan queue.WorkItem)

	st := &simpleQueueState{reqCh: q.reqCh, outCh: q.outCh, capacity: capacity}
	go st.loop()
	return q
}

type simpleQueue struct {
	// Immutable state, read by many goroutines
	reqCh chan queueReq
	outCh chan queue.WorkItem
}

type simpleQueueState struct {
	// Mutable state, only read/written by its goroutine
	reqCh     chan queueReq
	outCh     chan queue.WorkItem
	dequeueCh chan struct{}
	items     []*simpleWorkItem
	nextID    int
	capacity  int
}

func (s *simpleQueueState) loop() {
	for !s.done() {
		s.iter()

	}
	close(s.outCh)
}

func (s *simpleQueueState) done() bool {
	// We are done once our inputs are closed and we have no items left to send
	return s.reqCh == nil && len(s.items) == 0
}

func (s *simpleQueueState) iter() {
	var outCh chan queue.WorkItem
	var item *simpleWorkItem
	// only send if we aren't waiting for an ack and have something to send
	if s.dequeueCh == nil && len(s.items) > 0 {
		outCh = s.outCh
		item = s.items[0]
	}
	select {
	case outCh <- item:
		s.dequeueCh = item.dequeueCh
	case req, ok := <-s.reqCh:
		if !ok {
			s.reqCh = nil
			return
		}
		switch req := req.(type) {
		case enqueueReq:
			req.resultCh <- s.enqueue(req.job)
		case statusReq:
			req.resultCh <- s.status(req.jobId)
		}
	case <-s.dequeueCh:
		// Dequeue the last sent work item
		s.items = s.items[1:]
		s.dequeueCh = nil
	}
}

func (s *simpleQueueState) enqueue(job sched.Job) enqueueResult {
	if len(s.items) >= s.capacity {
		return enqueueResult{"", queue.NewCanNotScheduleNow(1*time.Second, "queue full")}
	}
	id := strconv.Itoa(s.nextID)
	s.nextID++
	job.Id = id
	item := &simpleWorkItem{job, make(chan struct{})}
	s.items = append(s.items, item)
	return enqueueResult{id, nil}
}

func (s *simpleQueueState) status(jobId string) statusResult {
	for _, item := range s.items {
		if item.job.Id == jobId {
			return statusResult{sched.PendingStatus(jobId), nil}
		}
	}
	return statusResult{sched.UnknownStatus(jobId), nil}
}

type simpleWorkItem struct {
	job       sched.Job
	dequeueCh chan struct{}
}

func (i *simpleWorkItem) Job() sched.Job {
	return sched.Job(i.job)
}

func (i *simpleWorkItem) Dequeue() {
	i.dequeueCh <- struct{}{}
	close(i.dequeueCh)
}

// Requests to our queue
type queueReq interface {
	queueReq()
}

type enqueueReq struct {
	job      sched.Job
	resultCh chan enqueueResult
}

func (r enqueueReq) queueReq() {}

type statusReq struct {
	jobId    string
	resultCh chan statusResult
}

func (r statusReq) queueReq() {}

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
	q.reqCh <- enqueueReq{job, resultCh}
	result := <-resultCh
	return result.jobID, result.err
}

type statusResult struct {
	status sched.JobStatus
	err    error
}

func (q *simpleQueue) Status(jobId string) (sched.JobStatus, error) {
	resultCh := make(chan statusResult)
	q.reqCh <- statusReq{jobId, resultCh}
	result := <-resultCh
	return result.status, result.err
}

func (q *simpleQueue) Chan() chan queue.WorkItem {
	return q.outCh
}

func (q *simpleQueue) Close() error {
	close(q.reqCh)
	return nil
}
