package local

import (
	"fmt"
	"strconv"

	"github.com/scootdev/scoot/runner"
	"golang.org/x/net/context"
	"strings"
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

//type mapEntry struct {
//	//runnerRunId  runner.RunId
//	runnerStatus runner.ProcessStatus
//	//errorMsg     string
//}

type runRequest struct {
	cmd      *runner.Command
	statusCh chan runner.ProcessStatus
	errCh    chan error
}

type QueueingRunner struct {
	delegate    runner.Runner
	maxQLen     int
	nextRunId   int // used to assign unique runIds to the run requests
	q           []commandAndId
	runnerAvail bool

	notifyRunnerAvailCh chan struct{} // the runner will use this channel to signal it is available

	runCh chan runRequest

	statusRequestCh  chan runner.RunId         // channel for synchronizing status requests
	statusResponseCh chan runner.ProcessStatus // channel for unblocking the status requests

	statusAllRequestCh  chan struct{}               // channel for synchroinizing status all requests
	statusAllResponseCH chan []runner.ProcessStatus // channel for unblocking status all requests

	abortRequestCh  chan runner.RunId         // channel for synchronizing abort requests
	abortResponseCh chan runner.ProcessStatus // channel for unblocking abort request

	eraseRequestCh  chan runner.RunId // channel for synchronizing erase requests
	eraseResponseCh chan error        // channel for unblocking erase requests

	// map the queueing runner run ids to the runner's run ids. The key is the queue's run id
	// and the id in the entrie's ProcessStatus is the runner's run id
	qIdToRunnerId map[string]runner.ProcessStatus

	ctx context.Context
}

// This must be used to initialize QueueingRunner properly
func NewQueuingRunner(context context.Context,
	theRunner runner.Runner,
	maxQueueLen int,
	runnerAvailableCh chan struct{}) runner.Runner {

	qRunner := &QueueingRunner{
		runCh:               make(chan runRequest),
		statusRequestCh:     make(chan runner.RunId),
		statusResponseCh:    make(chan runner.ProcessStatus),
		statusAllRequestCh:  make(chan struct{}),
		statusAllResponseCH: make(chan []runner.ProcessStatus),
		abortRequestCh:      make(chan runner.RunId),
		abortResponseCh:     make(chan runner.ProcessStatus),
		eraseRequestCh:      make(chan runner.RunId),
		eraseResponseCh:     make(chan error),

		delegate:            theRunner,
		maxQLen:             maxQueueLen,
		nextRunId:           0,
		runnerAvail:         true,
		notifyRunnerAvailCh: runnerAvailableCh,
		qIdToRunnerId:       make(map[string]runner.ProcessStatus),
		ctx:                 context,
	}

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
	qr.runCh <- runRequest{cmd, statusCh, errCh}
	return <-statusCh, <-errCh
}

// The 'events' include: Run() request, Status() request, or a signal that the runner is available.
// Run() events are put on the queue, Status() and runner available events are processed immediately
// Cycles that don't process Run(), Status() or runner avaialable events (default case) are used to
// run the next command in the queue.
func (qr *QueueingRunner) eventLoop() {
	for {

		select {
		case <-qr.ctx.Done():
			// stop processing commands
			return

		case req := <-qr.runCh:
			st, err := qr.addRequestToQueue(req.cmd)
			req.statusCh <- st
			req.errCh <- err

		case <-qr.notifyRunnerAvailCh:
			qr.runnerAvail = true

		case qRunId := <-qr.statusRequestCh:
			s := qr.getRunStatus(qRunId)
			qr.statusResponseCh <- s

		case <-qr.statusAllRequestCh:
			a := qr.getStatusAll()
			qr.statusAllResponseCH <- a

		case id := <-qr.abortRequestCh:
			s := qr.abortRun(id)
			qr.abortResponseCh <- s

		case id := <-qr.eraseRequestCh:
			err := qr.eraseRun(id)
			qr.eraseResponseCh <- err
		}

		if len(qr.q) > 0 && qr.runnerAvail {
			qr.runNextCommandInQueue()
		}

	}
}

// If there is room on the queue, assign a new runid and add the request to the queue.
// Otherwise return queue full error message
func (qr *QueueingRunner) addRequestToQueue(cmd *runner.Command) (runner.ProcessStatus, error) {

	if len(qr.q) >= qr.maxQLen {
		// the queue is full
		return runner.ProcessStatus{}, fmt.Errorf(QueueFullMsg)
	}

	// add the request to the queue
	runId := runner.RunId(strconv.Itoa(qr.nextRunId))
	qr.nextRunId++ // for next run request

	qr.q = append(qr.q, commandAndId{id: runId, cmd: cmd})

	s := runner.ProcessStatus{State: runner.PENDING}
	qr.qIdToRunnerId[string(runId)] = s // put runner's runid in the map

	return qr.makeStatusWithQId(runId, s), nil

}

// Run the first request on the queue and remove it from the queue
func (qr *QueueingRunner) runNextCommandInQueue() runner.ProcessStatus {

	request := qr.q[0]
	qr.q = qr.q[1:] // pop the top request off the queue

	rStatus, err := qr.delegate.Run(request.cmd) // run the command
	if err != nil {
		if err != nil {
			rStatus = qr.putErrorMsgInProcessStatus(rStatus, err.Error())
		}
	}

	qr.runnerAvail = false

	// update the map entry with the current state
	qr.qIdToRunnerId[string(request.id)] = rStatus

	return qr.makeStatusWithQId(request.id, rStatus)
}

// Status get the status of a run.
func (qr *QueueingRunner) Status(qRunId runner.RunId) (runner.ProcessStatus, error) {

	qr.statusRequestCh <- qRunId // block next status request

	s := <-qr.statusResponseCh // wait till get the status

	return s, nil
}

// the event loop is triggering getting the status
func (qr *QueueingRunner) getRunStatus(qRunId runner.RunId) runner.ProcessStatus {

	entry, ok := qr.qIdToRunnerId[string(qRunId)]

	if !ok {
		return runner.ProcessStatus{RunId: qRunId, State: runner.BADREQUEST, Error: UnknownRunIdMsg}
	}

	if entry.State == runner.PENDING || entry.State.IsDone() {
		return qr.makeStatusWithQId(qRunId, entry)
	}

	// get the current status from the runner
	s, err := qr.delegate.Status(entry.RunId)

	if err != nil {
		s = qr.putErrorMsgInProcessStatus(s, err.Error())
	}

	qr.qIdToRunnerId[string(qRunId)] = s

	return qr.makeStatusWithQId(qRunId, s)
}

// Current status of all runs, running and finished, excepting any Erase()'s runs.
func (qr *QueueingRunner) StatusAll() ([]runner.ProcessStatus, error) {
	qr.statusAllRequestCh <- struct{}{}

	a := <-qr.statusAllResponseCH
	return a, nil
}

func (qr *QueueingRunner) getStatusAll() []runner.ProcessStatus {
	var rVal []runner.ProcessStatus
	for qId, s := range qr.qIdToRunnerId {
		r := qr.makeStatusWithQId(runner.RunId(qId), s)
		rVal = append(rVal, r)
	}

	return rVal
}

func (qr *QueueingRunner) makeStatusWithQId(qId runner.RunId, s runner.ProcessStatus) runner.ProcessStatus {
	r := runner.ProcessStatus{RunId: runner.RunId(qId),
		State:     s.State,
		StdoutRef: s.StdoutRef,
		StderrRef: s.StderrRef,
		ExitCode:  s.ExitCode,
		Error:     s.Error}
	return r
}

func (qr *QueueingRunner) putErrorMsgInProcessStatus(ps runner.ProcessStatus, msg string) runner.ProcessStatus {
	if strings.Compare(ps.Error, "") == 0 {
		ps.Error = fmt.Sprintf("%s", msg)
	} else {
		ps.Error = fmt.Sprintf("%s, %s", ps.Error, msg)
	}
	return ps

}

// Kill the queued run if no runid is supplied kill all runs.
func (qr *QueueingRunner) Abort(runId runner.RunId) (runner.ProcessStatus, error) {
	qr.abortRequestCh <- runId

	s := <-qr.abortResponseCh

	return s, nil
}

func (qr *QueueingRunner) abortRun(runId runner.RunId) runner.ProcessStatus {
	qs, ok := qr.qIdToRunnerId[string(runId)]
	if !ok {
		return runner.ProcessStatus{RunId: runId, State: runner.BADREQUEST, Error: UnknownRunIdMsg}
	}

	if qs.State.IsDone() {
		// if its already done, don't do anything, return it's status
		return qr.makeStatusWithQId(runId, qs)
	}

	if qs.State == runner.PENDING {
		qs.State = runner.ABORTED

		for i, e := range qr.q {
			if e.id == runId {
				qr.q = append(qr.q[:i], qr.q[i+1:]...)
			}
		}
	} else {
		rs, err := qr.delegate.Abort(qs.RunId)
		if err != nil {
			qs = qr.putErrorMsgInProcessStatus(rs, err.Error())
		}
		qr.qIdToRunnerId[string(runId)] = qs
	}

	retS := qr.makeStatusWithQId(runId, qs)

	return retS

}

// Prunes the run history so StatusAll() can return a reasonable number of runs.
func (qr *QueueingRunner) Erase(runId runner.RunId) error {
	qr.eraseRequestCh <- runId

	e := <-qr.eraseResponseCh

	return e
}

func (qr *QueueingRunner) eraseRun(runId runner.RunId) error {
	rs, ok := qr.qIdToRunnerId[string(runId)]

	if !ok {
		return fmt.Errorf(UnknownRunIdMsg)
	}

	if !rs.State.IsDone() {
		return fmt.Errorf(RequestIsRunning, runId)
	}

	delete(qr.qIdToRunnerId, string(runId))

	return nil
}
