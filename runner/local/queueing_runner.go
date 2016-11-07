package local

// import (
// 	"fmt"
// 	"strconv"

// 	"github.com/scootdev/scoot/runner"
// )

// // TODO(dbentley): should this be in queue.go
// // TODO(dbentley): Move this whole package to runner/runners?

const QueueFullMsg = "No resources available. Please try later."
const UnknownRunIdMsg = "Unknown run id."
const RequestIsNotDone = "Run %s is not done, please Abort it first."

// // QueueingRunner manages a queue of commands.
// // One request is run at a time, via an underlying delegate runner
// // (We expect, for now, the delegate runner will ultimately be a simpleRunner
// // QueueingRunner will reject requests if the queue is full (i.e., its length == its capacity)
// type QueueingRunner struct {
// 	// channel for methods to send communication to eventLoop()
// 	reqCh chan interface{}

// 	// delegate that will run (we expect it to ultimately be a simpleRunner)
// 	delegate    runner.Runner
// 	capacity    int
// 	nextRunId   int // used to assign unique runIds to the run requests
// 	runnerAvail bool

// 	// Runs are in one of three meta-states in QueueingRunner:
// 	// *) queued
// 	// *) errored (we couldn't delegate them)
// 	// *) delegated

// 	// q holds queued commands (and assigned ids)
// 	q []commandAndId

// 	// errored holds info for runs that have errored.
// 	// We weren't able to send them to the delegate (either got an error from Run()
// 	// or they were aborted before running
// 	errored map[runner.RunId]runner.ProcessStatus

// 	// delegated runs are stored in both qToDel and delToQ. Together, these
// 	// form a BiMap (but Go doesn't make that easy), so we have two maps.
// 	// qToDel maps IDs in our namespace to IDs in the delegate's namespace
// 	// delToQ is vice versa
// 	qToDel map[runner.RunId]runner.RunId
// 	delToQ map[runner.RunId]runner.RunId
// }

// // NewQueueingRunner creates a new QueueingRunner.
// // delegate will be used to run.
// // capacity is the maximum entries to hold in the queue
// // runnerAvailableCh strobes when the delegate runner is available (from NewSimpleReportBackRunner)
// func NewQueuingRunner(delegate runner.Runner,
// 	capacity int,
// 	runnerAvailableCh chan struct{}) runner.Runner {

// 	qRunner := &QueueingRunner{
// 		reqCh: make(chan interface{}),

// 		delegate:  delegate,
// 		capacity:  capacity,
// 		nextRunId: 0,

// 		runnerAvail: true,

// 		q:       nil,
// 		qToDel:  make(map[runner.RunId]runner.RunId),
// 		delToQ:  make(map[runner.RunId]runner.RunId),
// 		errored: make(map[runner.RunId]runner.ProcessStatus),
// 	}

// 	// push runner available notifications into our request channel
// 	go func() {
// 		for st := range runnerAvailableCh {
// 			qRunner.reqCh <- st
// 		}
// 	}()

// 	// start the request event processor
// 	go qRunner.eventLoop()

// 	return qRunner
// }

// // Implement Runner's methods by sending a request to qr.reqCh and waiting

// func (qr *QueueingRunner) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
// 	statusCh, errCh := make(chan runner.ProcessStatus), make(chan error)
// 	qr.reqCh <- runRequest{cmd, statusCh, errCh}
// 	return <-statusCh, <-errCh
// }

// func (qr *QueueingRunner) Status(id runner.RunId) (runner.ProcessStatus, error) {
// 	statusCh, errCh := make(chan runner.ProcessStatus), make(chan error)
// 	qr.reqCh <- statusRequest{id, statusCh, errCh}
// 	return <-statusCh, <-errCh
// }

// func (qr *QueueingRunner) StatusAll() ([]runner.ProcessStatus, error) {
// 	statusesCh, errCh := make(chan []runner.ProcessStatus), make(chan error)
// 	qr.reqCh <- statusAllRequest{statusesCh, errCh}
// 	return <-statusesCh, <-errCh
// }

// func (qr *QueueingRunner) Abort(id runner.RunId) (runner.ProcessStatus, error) {
// 	statusCh, errCh := make(chan runner.ProcessStatus), make(chan error)
// 	qr.reqCh <- abortRequest{id, statusCh, errCh}
// 	return <-statusCh, <-errCh
// }

// func (qr *QueueingRunner) Erase(id runner.RunId) error {
// 	errCh := make(chan error)
// 	qr.reqCh <- eraseRequest{id, errCh}
// 	return <-errCh
// }

// commandAndId is a command waiting to run in the queue and the ID we've assigned
type commandAndId struct {
	id  runner.RunId
	cmd *runner.Command
}

// // Helper types for sending requests
// type runRequest struct {
// 	cmd      *runner.Command
// 	statusCh chan runner.ProcessStatus
// 	errCh    chan error
// }

// type statusRequest struct {
// 	id       runner.RunId
// 	statusCh chan runner.ProcessStatus
// 	errCh    chan error
// }

// type statusAllRequest struct {
// 	statusesCh chan []runner.ProcessStatus
// 	errCh      chan error
// }

// type abortRequest struct {
// 	id       runner.RunId
// 	statusCh chan runner.ProcessStatus
// 	errCh    chan error
// }

// type eraseRequest struct {
// 	id    runner.RunId
// 	errCh chan error
// }

// type closeRequest struct{}

// // Wait for input and respond (and then run the next command if applicable)
// func (qr *QueueingRunner) eventLoop() {
// 	for req := range qr.reqCh {
// 		switch req := req.(type) {
// 		case closeRequest:
// 			if !qr.runnerAvail {
// 				// drain reqCh
// 				// We need to let our delegate tell us we're available again, then we can close it
// 				<-qr.reqCh
// 			}
// 			close(qr.reqCh)
// 			return
// 		case struct{}:
// 			// struct{} is what comes from simpleRunner's runnerAvailable channel
// 			qr.runnerAvail = true
// 		case runRequest:
// 			st, err := qr.enqueue(req.cmd)
// 			req.statusCh <- st
// 			req.errCh <- err
// 		case statusRequest:
// 			st, err := qr.status(req.id)
// 			req.statusCh <- st
// 			req.errCh <- err
// 		case statusAllRequest:
// 			sts, err := qr.statusAll()
// 			req.statusesCh <- sts
// 			req.errCh <- err
// 		case abortRequest:
// 			st, err := qr.abort(req.id)
// 			req.statusCh <- st
// 			req.errCh <- err
// 		case eraseRequest:
// 			req.errCh <- qr.erase(req.id)
// 		default:
// 			panic(fmt.Errorf("unexpected value on QueueingRunner.reqCh: %T %v", req, req))
// 		}
// 		if len(qr.q) > 0 && qr.runnerAvail {
// 			qr.runNextCommandInQueue()
// 		}
// 	}
// }

// // If there is room on the queue, assign a new runid and add the request to the queue.
// // Otherwise return queue full error message
// func (qr *QueueingRunner) enqueue(cmd *runner.Command) (runner.ProcessStatus, error) {
// 	if len(qr.q) >= qr.capacity {
// 		// the queue is full
// 		return runner.ProcessStatus{}, fmt.Errorf(QueueFullMsg)
// 	}

// 	// add the request to the queue
// 	runId := runner.RunId(strconv.Itoa(qr.nextRunId))
// 	qr.nextRunId++ // for next run request

// 	qr.q = append(qr.q, commandAndId{id: runId, cmd: cmd})

// 	return runner.PendingStatus(runId), nil

// }

// // status, statusAll, abort, and erase all have the same structure:
// // try the delegate, then errored, then check the queue

// func (qr *QueueingRunner) status(id runner.RunId) (runner.ProcessStatus, error) {
// 	if delID, ok := qr.qToDel[id]; ok {
// 		st, err := qr.delegate.Status(delID)
// 		if err != nil {
// 			return runner.ProcessStatus{}, err
// 		}
// 		st.RunId = id
// 		return st, nil
// 	}

// 	if st, ok := qr.errored[id]; ok {
// 		return st, nil
// 	}

// 	for _, cmdAndID := range qr.q {
// 		if cmdAndID.id == id {
// 			return runner.PendingStatus(id), nil
// 		}
// 	}

// 	return runner.ProcessStatus{}, fmt.Errorf(UnknownRunIdMsg)
// }

// func (qr *QueueingRunner) statusAll() ([]runner.ProcessStatus, error) {
// 	stats, err := qr.delegate.StatusAll()
// 	if err != nil {
// 		return nil, err
// 	}

// 	// translate the RunID from the delegate's namespace to ours
// 	// (this is why we need the reverse mapping as well)
// 	for i, stat := range stats {
// 		delID := stat.RunId
// 		qID, ok := qr.delToQ[delID]
// 		if !ok {
// 			return nil, fmt.Errorf("Unknown run ID in delegate %v", delID)
// 		}
// 		stats[i].RunId = qID
// 	}

// 	for _, st := range qr.errored {
// 		stats = append(stats, st)
// 	}

// 	for _, cmdAndId := range qr.q {
// 		stats = append(stats, runner.PendingStatus(cmdAndId.id))
// 	}

// 	return stats, nil
// }

// func (qr *QueueingRunner) abort(id runner.RunId) (runner.ProcessStatus, error) {
// 	if delID, ok := qr.qToDel[id]; ok {
// 		return qr.delegate.Abort(delID)
// 	}

// 	if errSt, ok := qr.errored[id]; ok {
// 		return errSt, nil
// 	}

// 	for i, cmdAndId := range qr.q {
// 		if cmdAndId.id == id {
// 			// Run is queued. Set it as errored, and delete from queue
// 			st := runner.AbortStatus(id)
// 			qr.errored[id] = st
// 			qr.q = append(qr.q[:i], qr.q[i+1:]...)
// 			return st, nil
// 		}
// 	}

// 	return runner.ProcessStatus{}, fmt.Errorf(UnknownRunIdMsg)
// }

// func (qr *QueueingRunner) erase(id runner.RunId) error {
// 	if delID, ok := qr.qToDel[id]; ok {
// 		err := qr.delegate.Erase(delID)
// 		delete(qr.qToDel, id)
// 		delete(qr.delToQ, delID)
// 		return err
// 	}

// 	if _, ok := qr.errored[id]; ok {
// 		delete(qr.errored, id)
// 		return nil
// 	}

// 	for _, cmdAndId := range qr.q {
// 		if cmdAndId.id == id {
// 			return fmt.Errorf(RequestIsNotDone, id)
// 		}
// 	}

// 	return fmt.Errorf(UnknownRunIdMsg)
// }

// // Run the first request on the queue and remove it from the queue
// func (qr *QueueingRunner) runNextCommandInQueue() runner.ProcessStatus {
// 	request := qr.q[0]
// 	qr.q = qr.q[1:] // pop the top request off the queue

// 	st, err := qr.delegate.Run(request.cmd) // run the command
// 	if err != nil {
// 		errSt := runner.ErrorStatus(request.id, err)
// 		qr.errored[request.id] = errSt
// 		return errSt
// 	}

// 	qr.runnerAvail = false

// 	// request.id is now delegated, so update both foward and reverse map
// 	qr.qToDel[request.id] = st.RunId
// 	qr.delToQ[st.RunId] = request.id

// 	st.RunId = request.id
// 	return st
// }

// func (qr *QueueingRunner) Close() error {
// 	qr.reqCh <- closeRequest{}
// 	return nil
// }

type QueueController struct {
	statuses *Statuses
	invoker  *Invoker

	runnerID runner.RunId
	abortCh  chan struct{}
	queue    []commandAndID
	mu       sync.Mutex
}

func (c *QueueController) Run(cmd *Runner.Command) (runner.ProcessStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.runningID == "" {
		st := c.statuses.NewRun()

		return c.start(cmd, st.RunId)
	}

	return runner.ProcessStatus{}, fmt.Errorf(RunnerBusyMsg)
}
