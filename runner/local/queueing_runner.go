package local

import (
	"fmt"
	"golang.org/x/net/context"
	"strconv"

	"github.com/scootdev/scoot/runner"
)

// This runner manages multiple (possibly concurrent) run and status requests.
// One request is run at a time, additional requests are queued till the current request is finished running.

// QueueingRunner is initialized with the runner to use to run the requests, the maximum number of requests to
// manage on the queue, and the 'callback' channel the runner will use to tell queueing runner that it has
// completed the last request.
//
// When a request is received it is immediately added to the queue.  As the runner becomes available, if
// if there is are requests on the queue, the top request on the queue is sent to the runner
//
// If the max queue length is exceed, QueueingRunner rejects any new request till length of the queue is below
// the max.
//
// QueueingRunner assigns a unique id (run id) to each new request.  The Run() function returns this id and a
// status indicating if the request has been successfully queued or if the request has been denied (due to queue
// length exceeded or some other error).

// The status of a request is obtained with the run id.

const QueueFullMsg = "No resources available. Please try later."
const UnknownRunIdMsg = "Unknown run id."
const RequestIsRunning = "Run %s is still running, please Abort it first."

// The request queue entries are commandAndId structs
type commandAndId struct {
	id  runner.RunId
	cmd *runner.Command
}

type runRequest struct {
	cmd      *runner.Command
	statusCh chan runner.ProcessStatus
	errCh    chan error
}

type statusRequest struct {
	id       runner.RunId
	statusCh chan runner.ProcessStatus
	errCh    chan error
}

type statusAllRequest struct {
	statusesCh chan []runner.ProcessStatus
	errCh      chan error
}

type abortRequest struct {
	id       runner.RunId
	statusCh chan runner.ProcessStatus
	errCh    chan error
}

type eraseRequest struct {
	id    runner.RunId
	errCh chan error
}

type QueueingRunner struct {
	reqCh chan interface{}

	delegate    runner.Runner
	maxQLen     int
	nextRunId   int // used to assign unique runIds to the run requests
	runnerAvail bool

	qToDel  map[runner.RunId]runner.RunId
	delToQ  map[runner.RunId]runner.RunId
	errored map[runner.RunId]runner.ProcessStatus
	q       []commandAndId
}

// This must be used to initialize QueueingRunner properly
func NewQueuingRunner(context context.Context,
	delegate runner.Runner,
	maxQueueLen int,
	runnerAvailableCh chan struct{}) runner.Runner {

	qRunner := &QueueingRunner{
		reqCh: make(chan interface{}),

		delegate:  delegate,
		maxQLen:   maxQueueLen,
		nextRunId: 0,

		runnerAvail: true,

		q:       nil,
		qToDel:  make(map[runner.RunId]runner.RunId),
		delToQ:  make(map[runner.RunId]runner.RunId),
		errored: make(map[runner.RunId]runner.ProcessStatus),
	}

	go func() {
		for st := range runnerAvailableCh {
			q.reqCh <- st
		}
	}()

	// start the request event processor
	go qRunner.eventLoop()

	return qRunner
}

// Put the request and a callback channel (created for the request)
// on the queueing runner's processRequests channel.  When the request has been
// queued the request's runid, its queued status and any error will be returned
// via the request's callback channel
func (qr *QueueingRunner) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	statusCh, errCh := make(chan runner.ProcessStatus), make(chan error)
	qr.reqCh <- runRequest{cmd, statusCh, errCh}
	return <-statusCh, <-errCh
}

// Status get the status of a run.
func (qr *QueueingRunner) Status(id runner.RunId) (runner.ProcessStatus, error) {
	statusCh, errCh := make(chan runner.ProcessStatus), make(chan error)
	qr.reqCh <- statusRequest{id, statusCh, errCh}
	return <-statusCh, <-errCh
}

// Current status of all runs, running and finished, excepting any Erase()'s runs.
func (qr *QueueingRunner) StatusAll() ([]runner.ProcessStatus, error) {
	statusesCh, errCh := make(chan []runner.ProcessStatus), make(chan error)
	qr.reqCh <- statusAllRequest{statusCh, errCh}
	return <-statusesCh, <-errCh
}

// Kill the queued run
func (qr *QueueingRunner) Abort(runId runner.RunId) (runner.ProcessStatus, error) {
	statusCh, errCh := make(chan runner.ProcessStatus), make(chan error)
	qr.reqCh <- abortRequest{cmd, statusCh, errCh}
	return <-statusCh, <-errCh
}

// Prunes the run history so StatusAll() can return a reasonable number of runs.
func (qr *QueueingRunner) Erase(id runner.RunId) error {
	errCh := make(chan error)
	qr.reqCh <- eraseRequest{id, errCh}
	return <-errCh
}

// The 'events' include: Run() request, Status() request, or a signal that the runner is available.
// Run() events are put on the queue, Status() and runner available events are processed immediately
// Cycles that don't process Run(), Status() or runner avaialable events (default case) are used to
// run the next command in the queue.
func (qr *QueueingRunner) eventLoop() {
	for req := range qr.reqCh {
		switch req := req.(type) {
		case struct{}:
			qr.runnerAvail = true
		case runRequest:
			st, err := qr.addRequestToQueue(req.cmd)
			req.statusCh <- st
			req.errCh <- err
		case statusRequest:
			st, err := qr.status(req.id)
			req.statusCh <- st
			req.errCh <- err
		case statusAllRequest:
			sts, err := qr.statusAll()
			req.statusesCh <- sts
			req.errCh <- err
		case abortRequest:
			st, err := qr.abort(req.id)
			req.statusCh <- st
			req.errCh <- err
		case eraseRequest:
			req.errCh <- qr.erase(req.id)
		}
		if len(qr.q) > 0 && qr.runnerAvail {
			qr.runNextCommandInQueue()
		}
	}
}

// If there is room on the queue, assign a new runid and add the request to the queue.
// Otherwise return queue full error message
func (qr *QueueingRunner) enqueue(cmd *runner.Command) (runner.ProcessStatus, error) {
	if len(qr.q) >= qr.maxQLen {
		// the queue is full
		return runner.ProcessStatus{}, fmt.Errorf(QueueFullMsg)
	}

	// add the request to the queue
	runId := runner.RunId(strconv.Itoa(qr.nextRunId))
	qr.nextRunId++ // for next run request

	qr.q = append(qr.q, commandAndId{id: runId, cmd: cmd})

	return runner.PendingStatus(runId), nil

}

func (qr *QueueingRunner) status(id runner.RunId) (runner.ProcessStatus, error) {
	if st, ok := qr.errored[id]; ok {
		return st, nil
	}

	if delID, ok := qr.delegated[id]; ok {
		return qr.delegate.Status(delID)
	}

	for _, cmdAndID := range qr.q {
		if cmdAndID.id == id {
			return runner.PendingStatus(id), nil
		}
	}

	return runner.ProcessStatus{}, fmt.Errorf(UnknownRunIdMsg)
}

func (qr *QueueingRunner) getStatusAll() ([]runner.ProcessStatus, error) {
	r, err := qr.delegate.StatusAll()
	if err != nil {
		return nil, err
	}

	for i, st := range r {
		qID, ok := qr.delToQ[st.RunId]
		if !ok {
			return nil, fmt.Errorf("Unknown run ID in delegate %v", st.RunId)
		}
		r[i].RunId = qID
	}

	for _, cmdAndId := range qr.q {
		r = append(r, runner.PendingStatus(cmdAndId.id))
	}

	for _, st := range qr.error {
		r = append(r, st)
	}

	return r, nil
}

func (qr *QueueingRunner) abort(id runner.RunId) (runner.ProcessStatus, error) {
	if delID, ok := qr.qToDel[id]; ok {
		return qr.del.Abort(delID)
	}

	if errSt, ok := q.errored[id]; ok {
		return errSt, nil
	}

	for i, cmdAndId := range qr.q {
		if cmdAndId.id == id {
			// Run is queued. Set it as erroed, and delete from queue
			st := runner.AbortStatus(id)
			qr.errored[id] = st
			qr.q = append(qr.q[:i], qr.q[i+1:]...)
			return st, nil
		}
	}

	return runner.ProcessStatus{}, fmt.Errorf(UnknownRunIdMsg)
}

func (qr *QueueingRunner) erase(runId runner.RunId) error {
	if delID, ok := qr.qToDel[id]; ok {
		err := qr.delegate.Erase(delID)
		delete(qr.qToDel, id)
		delete(qr.delToQ, delID)
		return err
	}

	if _, ok := qr.errored[id]; ok {
		delete(qr.errored, id)
		return nil
	}

	for _, cmdAndId := range qr.q {
		if cmdAndId.id == id {
			return fmt.Errorf(RequestIsRunning, id)
		}
	}

	return fmt.Errorf(UnknownRunIdMsg)
}

// Run the first request on the queue and remove it from the queue
func (qr *QueueingRunner) runNextCommandInQueue() runner.ProcessStatus {
	request := qr.q[0]
	qr.q = qr.q[1:] // pop the top request off the queue

	st, err := qr.delegate.Run(request.cmd) // run the command
	if err != nil {
		errSt := runner.ErrorStatus(request.id, err)
		qr.errored[request.id] = errSt
		return errSt
	}

	qr.runnerAvail = false

	// update the map entry with the current state
	qr.qToDel[request.id] = st.RunId
	qr.delToQ[st.RunId] = request.id

	st.RunId = request.id
	return st
}
