package memory

// In-memory Scheduler Queue

import (
	"strconv"
	"time"

	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
)

func NewSimpleQueue(capacity int) queue.Queue {
	q := &simpleQueue{}
	q.inCh = make(chan enqueueReq)
	q.outCh = make(chan queue.WorkItem)

	st := &simpleQueueState{inCh: q.inCh, outCh: q.outCh, capacity: capacity}
	go st.loop()
	return q
}

type simpleQueue struct {
	// Immutable state, read by many goroutines
	inCh  chan enqueueReq
	outCh chan queue.WorkItem
}

type simpleQueueState struct {
	// Mutable state, only read/written by its goroutine
	inCh      chan enqueueReq
	outCh     chan queue.WorkItem
	respondCh chan bool
	items     []*simpleWorkItem
	nextID    int
	capacity  int
}

func (s *simpleQueueState) loop() {
	for !s.done() {
		var outCh chan queue.WorkItem
		var item *simpleWorkItem
		// only send if we aren't waiting for an ack and have something to send
		if s.outCh != nil && s.respondCh == nil && len(s.items) > 0 {
			outCh = s.outCh
			item = s.items[0]
		}
		select {
		case outCh <- item:
			s.respondCh = item.respondCh
		case req, ok := <-s.inCh:
			if !ok {
				s.inCh = nil
				close(s.outCh)
				s.outCh = nil
				continue
			}
			req.resultCh <- s.enqueue(req.def)
		case claimed := <-s.respondCh:
			if claimed {
				// Dequeue the last sent work item
				s.items = s.items[1:]
			}
			s.respondCh = nil
		}
	}
	if s.respondCh != nil {
		<-s.respondCh
	}
}

func (s *simpleQueueState) done() bool {
	// We are done once our inputs are closed and we have no items left to send
	return s.inCh == nil
}

func (s *simpleQueueState) iter() {
}

func (s *simpleQueueState) enqueue(def sched.JobDefinition) enqueueResult {
	if len(s.items) >= s.capacity {
		return enqueueResult{"", queue.NewCanNotScheduleNow(1*time.Second, "queue full")}
	}
	id := strconv.Itoa(s.nextID)
	s.nextID++
	job := sched.Job{Id: id, Def: def}
	item := &simpleWorkItem{job, make(chan bool)}
	s.items = append(s.items, item)
	return enqueueResult{id, nil}
}

type simpleWorkItem struct {
	job       sched.Job
	respondCh chan bool
}

func (i *simpleWorkItem) Job() sched.Job {
	return i.job
}

func (i *simpleWorkItem) Respond(claimed bool) error {
	i.respondCh <- claimed
	close(i.respondCh)
	return nil
}

type enqueueReq struct {
	def      sched.JobDefinition
	resultCh chan enqueueResult
}

type enqueueResult struct {
	jobID string
	err   error
}

func (q *simpleQueue) Enqueue(job sched.JobDefinition) (string, error) {
	err := queue.ValidateJob(job)
	if err != nil {
		return "", err
	}
	resultCh := make(chan enqueueResult)
	q.inCh <- enqueueReq{job, resultCh}
	result := <-resultCh
	return result.jobID, result.err
}

func (q *simpleQueue) Chan() chan queue.WorkItem {
	return q.outCh
}

func (q *simpleQueue) Close() error {
	close(q.inCh)
	return nil
}
