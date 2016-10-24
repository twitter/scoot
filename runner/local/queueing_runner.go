package local

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/scootdev/scoot/runner"
	"golang.org/x/net/context"
)

// This runner manages multiple (possibly concurrent) run requests.  One request is run at a time, queueing additional
// requests till the current request is finished running.  QueueingRunner is initialized with the runner it will use
// to run the requests and the maximum number of requests to manage on the queue.
//
// When a request is received it is immediately added to the queue.  QueueingRunner uses a background go routine to
// pull a request off the queue and wait for a runner to become available to run it.
//
// If the max queue length is exceed, QueueingRunner rejects any new request till length of the queue is below
// the max.
//
// QueueingRunner will assign a unique id (run id) to each new request.  The Run() function will return this id and a
// status indicating if the request has been successfully queued or if the request has been denied (due to queue lenght
// exceeded or some other error encountered).

// The status of a request can be obtained using the run id.

const QueueFullMsg = "No resources available. Please try later."
const UnspportedFeatureMsg = "Not implemented.  This feature is not available for QueueingRunner"
const NoStatusMsg = "No status was found for run id %s, is the run id correct?"
const StatusErrMsg = "An error occurred trying to get the status for run id %s" // this would be a 'contact support team' message
const QueuedMsg = "Request with run id %s is queued for running."
const WaitForRunner = 500 * time.Millisecond // Note: if delay is > 500ms, the code does not pass the race test

var idCounterMu sync.RWMutex
var mapEntryMu sync.Mutex

// this is the struct that is put in the queue (channel)
type commandAndId struct {
	id  string // Note: tried using runner.RunId - had problems with using it as map key
	cmd *runner.Command
}

// this struct is used to map QueueingRunner runIds to the runner's runId and to hold results status
// for status queries
type requestAndResult struct {
	cmd    *runner.Command
	result *runner.ProcessStatus
}

type QueueingRunner struct {
	runner      runner.Runner
	maxQueueLen int
	nextRunId   int64 // used to assign unique runIds to the run requests
	queue       chan commandAndId
	requestMap  map[string]*requestAndResult //TODO - get help from someone who knows go: tried to use runner.RunId as key, was getting type runner.Runner has no field or method RunId error
}

// this func must be used to start the listener go routine and initialize QueueingRunner properly
func NewQueuingRunner(context context.Context, runner runner.Runner, maxQueueLen int) runner.Runner {
	q := make(chan commandAndId, maxQueueLen)
	rMap := make(map[string]*requestAndResult)
	qRunner := &QueueingRunner{runner: runner, maxQueueLen: maxQueueLen, nextRunId: 0, queue: q, requestMap: rMap}

	// start the queue listener
	go qRunner.runRequestedCommands(context, q)

	return qRunner
}

// create the queue's runid for the request and put the request on the queue
// put an empty entry in the request map and return the queue's runid
// calls to this function must be synchronized around creating the unique runid's
// and updating the map
func (qr QueueingRunner) Run(c *runner.Command) (runner.ProcessStatus, error) {
	log.Printf("local.queueingRunner: in qr.Run() args:%v\n", c.Argv)

	//if the queue is full, reject the command
	if len(qr.queue) == qr.maxQueueLen {
		return runner.ProcessStatus{}, fmt.Errorf(QueueFullMsg)
	}

	qRunId := qr.addRequestToQueue(c)

	// put an entry in the request map with an empty process status
	// The empty process status indicates that the request is queued,
	// it will be replaced with the runner's process status
	// when the run is actually starting.
	// this action must lock acces to the map
	log.Printf("*****local.QueueingRunner.Run: adding %s, %v to map\n", qRunId, c.Argv)
	mapEntryMu.Lock()
	qr.requestMap[qRunId] = &requestAndResult{cmd: c, result: &runner.ProcessStatus{}}
	mapEntryMu.Unlock()

	// create a return process status with this runners runid
	processStatusForReturn := runner.ProcessStatus{RunId: runner.RunId(qRunId)}

	return processStatusForReturn, nil
}


func (qr QueueingRunner)addRequestToQueue(c *runner.Command) string {
	idCounterMu.Lock()
	defer idCounterMu.Unlock()
	qRunId := fmt.Sprintf("%d", qr.nextRunId)

	// create a commandAndId
	cmdAndId := commandAndId{id: qRunId, cmd: c}

	// put it on the queue
	qr.queue <- cmdAndId

	qr.nextRunId = qr.nextRunId + 1
	log.Printf("current runid: %s, next run id:%d\n",qRunId, qr.nextRunId)

	return qRunId
}


// get a request from the queue and wait till the runner is available to run it.
func (qr QueueingRunner) runRequestedCommands(context context.Context, requestQueue <-chan commandAndId) {

	// pull the next request off the queue and wait till a runner is available to run it
	for runRequest := range requestQueue {

		log.Printf("***** local.QueueingRunner.runRequestedCommands: pulled %s, %v off the queue", runRequest.id, runRequest.cmd.Argv)
		// loop till a runner is found for this request
		lookingForAvailableRunner := true
		for lookingForAvailableRunner {
			status, err := qr.runner.Run(runRequest.cmd)

			if err == nil {
				// the runner is running the command
				lookingForAvailableRunner = false
				mapEntryMu.Lock()
				requestAndResult := qr.requestMap[runRequest.id]
				requestAndResult.result = &status
				mapEntryMu.Unlock()

			} else if strings.Compare(runner.RunnerBusyMsg, err.Error()) != 0 {
				// the runner returned an unexpected error
				lookingForAvailableRunner = false
				//TODO - is this the right behavoir? should it be FAILED or BADREQUEST?  also we are swallowing the return status
				status := runner.ProcessStatus{RunId: runner.RunId(runRequest.id), State: runner.BADREQUEST, Error: err.Error()}
				mapEntryMu.Lock()
				requestAndResult := qr.requestMap[runRequest.id]
				requestAndResult.result = &status
				mapEntryMu.Unlock()
			} else {
				// runner is busy so sleep and try again
				time.Sleep(WaitForRunner)
			}
		}

		// the current request has been started or rejected
	}
}

// Status get the status of a run.
func (qr QueueingRunner) Status(qRunId runner.RunId) (runner.ProcessStatus, error) {
	runIdStr := fmt.Sprintf("%s", qRunId)
	mapEntryMu.Lock()
	defer mapEntryMu.Unlock()
	mapEntry := qr.requestMap[runIdStr]

	// if the map does not have an entry for run id return an error
	if *mapEntry == (requestAndResult{}) {
		return runner.ProcessStatus{}, fmt.Errorf(NoStatusMsg, qRunId)
	}

	// if the map entry has no status info, return that it is queued
	if mapEntry.result.State == runner.UNKNOWN {
		// TODO - make 'queued' at state in ProcessState?
		return runner.ProcessStatus{}, fmt.Errorf(QueuedMsg, qRunId)
	}

	// if the run was already determined to be done (in a prior status request),
	// return the entry in the queue's map (after making sure the result has the queue's runid)
	if mapEntry.result.State.IsDone() {
		mapEntry.result.RunId = qRunId
		return *mapEntry.result, nil
	}

	// get its current status from the runner using the runner's runid
	mostRecentStatus, err := qr.runner.Status(mapEntry.result.RunId)
	if err != nil {
		return runner.ProcessStatus{}, fmt.Errorf(StatusErrMsg, mapEntry.result.RunId)
	}

	if mostRecentStatus.State.IsDone() {
		mapEntry.result.RunId = qRunId
	}
	// replace the runner's status with the most recent one and runid with the one assigned by the queue
	// TODO - discuss with team - the status is now being kept in 2 locations, the
	// actual runner's map and this queueing runner's map,  the next two lines will cause the
	// queueing runner to update it's status with the acutal runner's value
	mapEntry.result.State = mostRecentStatus.State
	mapEntry.result.StderrRef = mostRecentStatus.StderrRef
	mapEntry.result.StdoutRef = mostRecentStatus.StdoutRef
	mapEntry.result.Error = mostRecentStatus.Error
	mapEntry.result.ExitCode = mostRecentStatus.ExitCode
	return *mapEntry.result, nil

}

// Current status of all runs, running and finished, excepting any Erase()'s runs.
func (qr QueueingRunner) StatusAll() ([]runner.ProcessStatus, error) {
	processStatus := runner.ProcessStatus{}
	statuses := []runner.ProcessStatus{processStatus}
	return statuses, fmt.Errorf(UnspportedFeatureMsg)
}

// Kill the given run.
func (qr QueueingRunner) Abort(run runner.RunId) (runner.ProcessStatus, error) {
	return runner.ProcessStatus{}, fmt.Errorf(UnspportedFeatureMsg)
}

// Prunes the run history so StatusAll() can return a reasonable number of runs.
func (qr QueueingRunner) Erase(run runner.RunId) error {
	return fmt.Errorf(UnspportedFeatureMsg)
}

