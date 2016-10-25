package local

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/scootdev/scoot/runner"
	"golang.org/x/net/context"
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
const UnspportedFeatureMsg = "Not implemented.  This feature is not available for QueueingRunner"
const UnknownRunIdMsg = "Unknown run id."

// The request queue entries are commandAndId structs
type commandAndId struct {
	id  runner.RunId
	cmd *runner.Command
}

// requestResponse structs are returned in the Run() and Status() response channels
type requestResponse struct {
	status *runner.ProcessStatus
	err    error
}

//TODO - do we really need a new channel for each run request?
// This struct to associates a response channel to each Run() request
type runRequestEvent struct {
	cmd               *runner.Command
	onQueueResponseCh chan requestResponse // chan to use to indicate that the request is queued
}

type mapEntry struct {
	runnerRunId runner.RunId
	state       runner.ProcessState
	errorMsg    string
}

type QueueingRunner struct {
	runner            runner.Runner
	done              bool
	maxQueueLen       int
	nextRunId         int // used to assign unique runIds to the run requests
	runQueue          []commandAndId
	runnerIsAvailable bool

	notifyRunnerAvailCh chan struct{} // the runner will use this channel to signal it is available

	runRequestsCh chan runRequestEvent // channel for synchronizing calls to Run()

	statusRequestsCh chan string          // channel for synchronizing status requests
	statusResponseCh chan requestResponse // channel for unblocking the status requests

	// map the queueing runner run ids to the runner's run ids. We need runner's run id for Status()
	queueIdToRunnerIdMap map[string]mapEntry
}

// This must be used to initialize QueueingRunner properly
func NewQueuingRunner(context context.Context,
	runner runner.Runner,
	maxQueueLen int,
	runnerAvailableCh chan struct{}) runner.Runner {

	qRunner := &QueueingRunner{
		runner:               runner,
		done:                 false,
		maxQueueLen:          maxQueueLen,
		nextRunId:            0,
		runnerIsAvailable:    true,
		notifyRunnerAvailCh:  runnerAvailableCh,
		runRequestsCh:        make(chan runRequestEvent),
		statusRequestsCh:     make(chan string),
		statusResponseCh:     make(chan requestResponse),
		queueIdToRunnerIdMap: make(map[string]mapEntry),
	}

	// start the request event processor
	go qRunner.eventLoop()

	return qRunner
}

// Put the request and a callback channel (created for the request)
// on the queueing runner's processRequests channel.  When the request has been
// queued the request's runid, its queued status and any error will be returned
// via the request's callback channel
func (qr *QueueingRunner) Run(c *runner.Command) (runner.ProcessStatus, error) {
	log.Printf("local.queueingRunner: in qr.Run() args:%v\n", c.Argv)

	// make a channel for the 'on queue' response
	requestOnQueueCh := make(chan requestResponse)

	// put the request on the process requests channel
	requestContent := runRequestEvent{cmd: c, onQueueResponseCh: requestOnQueueCh}
	qr.runRequestsCh <- requestContent

	// get and return the 'on queue' response from this request's channel
	requestQueued := <-requestOnQueueCh
	close(requestOnQueueCh)
	return *requestQueued.status, requestQueued.err
}

// The 'events' include: Run() request, Status() request, or a signal that the runner is available.
// Run() events are put on the queue, Status() and runner available events are processed immediately
// Cycles that don't process Run(), Status() or runner avaialable events (default case) are used to
// run the next command in the queue.
func (qr *QueueingRunner) eventLoop() {
	for !qr.done {

		select {
		case nextRequest := <-qr.runRequestsCh:
			qr.addRequestToQueue(nextRequest)

		case <-qr.notifyRunnerAvailCh:
			qr.runnerIsAvailable = true

		case qRunId := <-qr.statusRequestsCh:
			mapEntry := qr.queueIdToRunnerIdMap[qRunId]
			qr.getRunStatus(mapEntry, runner.RunId(qRunId))

		default:
			if len(qr.runQueue) > 0 && qr.runnerIsAvailable {
				qr.runnerIsAvailable = false
				qr.runNextCommandInQueue()
			}
		}
	}
}

// If there is room on the queue, assign a new runid and add the request to the queue.
// Otherwise return queue full error message
func (qr *QueueingRunner) addRequestToQueue(request runRequestEvent) {

	if len(qr.runQueue) == qr.maxQueueLen {
		// the queue is full
		s := runner.ProcessStatus{State: runner.FAILED, Error: QueueFullMsg}
		request.onQueueResponseCh <- requestResponse{status: &s, err: fmt.Errorf(QueueFullMsg)}
		return
	}

	// add the request to the queue
	runId := runner.RunId(strconv.Itoa(qr.nextRunId))
	commandAndId := commandAndId{id: runId, cmd: request.cmd}
	qr.runQueue = append(qr.runQueue, commandAndId)

	qr.queueIdToRunnerIdMap[string(runId)] = mapEntry{state: runner.PENDING} // put runner's runid in the map

	qr.nextRunId++ // for next run request

	// use the request's response channel to return the run id and 'pending' status
	s := runner.ProcessStatus{RunId: runId, State: runner.PENDING}
	request.onQueueResponseCh <- requestResponse{status: &s, err: nil}

}

// Run the first request on the queue and remove it from the queue
func (qr *QueueingRunner) runNextCommandInQueue() {

	request := qr.runQueue[0]
	rStatus, err := qr.runner.Run(request.cmd) // run the command
	if err != nil {
		mapEntry := mapEntry{state: runner.BADREQUEST, errorMsg: err.Error()}
		qr.queueIdToRunnerIdMap[string(request.id)] = mapEntry // update the map entry with the current state
		return
	}

	mapEntry := mapEntry{runnerRunId: rStatus.RunId, state: rStatus.State}
	qr.queueIdToRunnerIdMap[string(request.id)] = mapEntry // update the map entry with the current state

	qr.runQueue = qr.runQueue[1:] // pop the top request off the queue
}

// Status get the status of a run.
func (qr *QueueingRunner) Status(qRunId runner.RunId) (runner.ProcessStatus, error) {

	qr.statusRequestsCh <- string(qRunId) // block next status request

	statusResponse := <-qr.statusResponseCh // wait till get the status

	return *statusResponse.status, statusResponse.err
}

// the event loop is triggering getting the status
func (qr *QueueingRunner) getRunStatus(entry mapEntry, qRunId runner.RunId) {
	if (entry == mapEntry{}) {
		s := runner.ProcessStatus{RunId: qRunId, State: runner.BADREQUEST, Error: UnknownRunIdMsg}
		qr.statusResponseCh <- requestResponse{status: &s, err: nil}
		return
	}

	if strings.Compare(runner.PENDING.String(), entry.state.String()) == 0 {
		s := runner.ProcessStatus{RunId: qRunId, State: runner.PENDING}
		qr.statusResponseCh <- requestResponse{status: &s, err: nil}
		return
	}

	if entry.state == runner.BADREQUEST {
		s := runner.ProcessStatus{RunId: qRunId, State: runner.BADREQUEST, Error: entry.errorMsg}
		qr.statusResponseCh <- requestResponse{status: &s, err: nil}
		return
	}

	runnerStatus, err := qr.runner.Status(entry.runnerRunId) // get the current status from runner

	runnerStatus.RunId = runner.RunId(qRunId) //overwrite the runner's runid with the queuing runner's runid

	qr.statusResponseCh <- requestResponse{status: &runnerStatus, err: err} // put the status on the return channel
}

// Current status of all runs, running and finished, excepting any Erase()'s runs.
func (qr *QueueingRunner) StatusAll() ([]runner.ProcessStatus, error) {
	return []runner.ProcessStatus{runner.ProcessStatus{}}, fmt.Errorf(UnspportedFeatureMsg)
}

// Kill the queued run if no runid is supplied kill all runs.
func (qr *QueueingRunner) Abort(run runner.RunId) (runner.ProcessStatus, error) {
	if run == runner.RunId("") {
		qr.done = true
		return runner.ProcessStatus{}, nil
	}

	//TODO implement killing one run
	return runner.ProcessStatus{RunId: run}, fmt.Errorf(UnspportedFeatureMsg)

}

// Prunes the run history so StatusAll() can return a reasonable number of runs.
func (qr *QueueingRunner) Erase(run runner.RunId) error {
	return fmt.Errorf(UnspportedFeatureMsg)
}
