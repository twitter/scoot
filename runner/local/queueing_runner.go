package local

import (
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/scootdev/scoot/runner"
	"golang.org/x/net/context"

	"strings"
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
const QueuedMsg = "Request with run id %s is queued for running."

// this is the struct that is put in the queue (channel)
type commandAndId struct {
	id  string // Note: tried using runner.RunId - had problems with using it as map key
	cmd *runner.Command
}


type QueueingRunner struct {
	runner      runner.Runner
	maxQueueLen int
	nextRunId   int // used to assign unique runIds to the run requests
	runRequests []commandAndId
	mu sync.Mutex
	done bool
	jobIsRunning bool
	runnerDoneCh chan runner.RunId

	queueIdToRunnerIdMap  map[string]string
}

// this func must be used to start the listener go routine and initialize QueueingRunner properly
func NewQueuingRunner(context context.Context, runner runner.Runner, maxQueueLen int, runDoneCh chan runner.RunId) runner.Runner {
	rMap := make(map[string]string)
	qRunner := &QueueingRunner{runner: runner, maxQueueLen: maxQueueLen, nextRunId: 0, done: false, jobIsRunning:false,queueIdToRunnerIdMap: rMap, runnerDoneCh:runDoneCh}

	// start the queue listener
	go qRunner.runRequestedCommands(context)

	return qRunner
}

// create the queue's runid for the request and put the request on the queue
func (qr *QueueingRunner) Run(c *runner.Command) (runner.ProcessStatus, error) {
	log.Printf("local.queueingRunner: in qr.Run() args:%v\n", c.Argv)

	qr.mu.Lock()
	defer qr.mu.Unlock()

	//if the queue is full, reject the command
	if len(qr.runRequests) > qr.maxQueueLen {
		return runner.ProcessStatus{}, fmt.Errorf(QueueFullMsg)
	}

	qRunId := strconv.Itoa(qr.nextRunId)

	// create a commandAndId
	cmdAndId := commandAndId{id: qRunId, cmd: c}

	// put it on the queue
	qr.runRequests = append(qr.runRequests, cmdAndId)

	qr.nextRunId = qr.nextRunId + 1

	return runner.ProcessStatus{RunId:runner.RunId(qRunId)}, nil
}



// if there is a request
func (qr *QueueingRunner) runRequestedCommands(context context.Context) {

	// loop till the queueing runner is aborted
	for !qr.done {

		if (len(qr.runRequests) > 0) {
			// start runRequest[0]
			request := qr.runRequests[0]
			r, err := qr.runner.Run(request.cmd)
			if err == nil {
				// put runner's runid in the map
				qr.queueIdToRunnerIdMap[request.id] = qr.runIdToString(r.RunId)

				// pop runRquest[0] off the list
				qr.runRequests = qr.runRequests[1:]

				// wait till runner returns the id on the runnerDoneCh
				<-qr.runnerDoneCh
			}
		}
	}

}

// Status get the status of a run.
func (qr *QueueingRunner) Status(qRunId runner.RunId) (runner.ProcessStatus, error) {

	runnerRunId := qr.queueIdToRunnerIdMap[qr.runIdToString(qRunId)]
	// if the runId is in the queued list return queue
	if qr.inQueuedList(qRunId) {
		err := fmt.Errorf(QueuedMsg, qr.runIdToString(qRunId))
		return runner.ProcessStatus{RunId:qRunId}, err
	}
	runnerStatus, err := qr.runner.Status(runner.RunId(runnerRunId))
	return runner.ProcessStatus{RunId:qRunId, StdoutRef:runnerStatus.StdoutRef, StderrRef:runnerStatus.StderrRef, State:runnerStatus.State, ExitCode:runnerStatus.ExitCode, Error:runnerStatus.Error}, err

}

func (qr *QueueingRunner) inQueuedList(runId runner.RunId) bool {
	for  _, runRequest := range qr.runRequests {
		if strings.Compare(runRequest.id, qr.runIdToString(runId)) == 0{
			return true
		}
	}

	return false
}

func (qr *QueueingRunner)runIdToString(runId runner.RunId) string {
	return fmt.Sprintf("%s", runId)
}

// Current status of all runs, running and finished, excepting any Erase()'s runs.
func (qr *QueueingRunner) StatusAll() ([]runner.ProcessStatus, error) {
	processStatus := runner.ProcessStatus{}
	statuses := []runner.ProcessStatus{processStatus}
	return statuses, fmt.Errorf(UnspportedFeatureMsg)
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

