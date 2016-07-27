package server

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/runner"
	execer "github.com/scootdev/scoot/runner/execer/os"
	localrunner "github.com/scootdev/scoot/runner/local"
	domain "github.com/scootdev/scoot/workerapi"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

// Called by a main binary. Blocks until the connection is terminated.
func Serve(handler worker.Worker, addr string, transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) error {
	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		return err
	}
	processor := worker.NewWorkerProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	fmt.Println("Serving thrift: ", addr)

	return server.Serve()
}

type leaseTimer struct {
	lease time.Duration
	timer *time.Timer
}

type handler struct {
	stat       stats.StatsReceiver
	run        runner.Runner
	getVersion func() string
	numRuns    int
	running    map[string]*domain.RunStatus
	ended      map[string]*domain.RunStatus
	leases     map[string]*leaseTimer
	inCh       chan func()
}

func DefaultHandler() worker.Worker {
	return NewHandler(nil, nil, nil)
}

func NewHandler(stat stats.StatsReceiver, run runner.Runner, getVersion func() string) worker.Worker {
	if stat == nil {
		stat = stats.NilStatsReceiver()
	}
	if run == nil {
		run = localrunner.NewSimpleRunner(execer.NewExecer())
	}
	if getVersion == nil {
		getVersion = func() string { return "" }
	}
	scopedStat := stat.Scope("handler")
	running := make(map[string]*domain.RunStatus)
	ended := make(map[string]*domain.RunStatus)
	leases := make(map[string]*leaseTimer)

	handler := &handler{stat: scopedStat, run: run, getVersion: getVersion,
		numRuns: 0, running: running, ended: ended, leases: leases, inCh: make(chan func())}
	go handler.loop()
	return handler
}

func processToRunStatus(process *runner.ProcessStatus, runId string) *domain.RunStatus {
	//NOTE: this relies on 'reasonable' ProcessStatus defaults in the error cases.
	rs := domain.NewRunStatus()
	rs.RunId = runId
	rs.OutUri = process.StdoutRef
	rs.ErrUri = process.StderrRef
	rs.ExitCode = process.ExitCode
	rs.Info = process.Error
	switch process.State {
	case runner.UNKNOWN:
		rs.Status = domain.Status_UNKNOWN
	case runner.PENDING:
		rs.Status = domain.Status_PENDING
	case runner.RUNNING:
		rs.Status = domain.Status_RUNNING
	case runner.COMPLETE, runner.FAILED:
		rs.Status = domain.Status_COMPLETED
	default:
		//TODO: add timeout support to runner and handle here.
		panic(fmt.Sprintf("Unexpected ProcessState %v", int(process.State)))
	}
	return rs
}

func (h *handler) doCleanupRunImpl(runId string) {
	h.ended[runId] = h.running[runId]
	delete(h.running, runId)
	if lease, ok := h.leases[runId]; ok {
		lease.timer.Stop()
		delete(h.leases, runId)
	}
}

func (h *handler) doAbortImpl(runId string) *domain.RunStatus {
	process, err := h.run.Abort(runner.RunId(runId))
	if err != nil {
		panic(fmt.Sprintf("Runner doesn't know about runId: %s", runId))
	}
	rs := processToRunStatus(&process, runId)
	rs.Status = domain.Status_ABORTED
	h.running[runId] = rs
	h.doCleanupRunImpl(runId)
	return rs
}

// Called as thunks in the thrift rpc functions, and run in the loop() goroutine.
//
func (h *handler) doRun(cmd *domain.RunCommand, resultCh chan *domain.RunStatus) {
	timeout := time.Millisecond * time.Duration(cmd.TimeoutMs)
	lease := time.Millisecond * time.Duration(cmd.LeaseMs)
	runcmd := runner.Command{Argv: cmd.Argv, EnvVars: nil, Timeout: timeout}
	process, err := h.run.Run(&runcmd)

	runId := strconv.Itoa(h.numRuns)
	rs := processToRunStatus(&process, runId)
	h.running[runId] = rs
	h.numRuns++
	if err != nil {
		h.stat.Counter("runFailures").Inc(1)
		rs.Info = err.Error()
		h.doCleanupRunImpl(runId)
	} else if lease != 0 {
		timer := time.AfterFunc(lease, func() {
			h.inCh <- func() {
				process, err = h.run.Status(runner.RunId(runId))
				if err == nil && !process.State.IsDone() {
					rs = h.doAbortImpl(runId)
					rs.Status = domain.Status_ORPHANED
				}
			}
		})
		h.leases[runId] = &leaseTimer{lease, timer}
	}

	resultCh <- rs
}

func (h *handler) doQueryWorker(resultCh chan *domain.WorkerStatus) {
	ws := domain.NewWorkerStatus()
	for runId, _ := range h.running {
		ws.Running = append(ws.Running, runId)
	}
	for runId, _ := range h.ended {
		ws.Ended = append(ws.Ended, runId)
	}
	ws.VersionId = h.getVersion()
	resultCh <- ws
}

func (h *handler) doQueryRun(runId string, resultCh chan *domain.RunStatus) {
	if status, ok := h.running[runId]; ok {
		if lease, ok := h.leases[runId]; ok {
			lease.timer.Reset(lease.lease)
		}
		resultCh <- status
		return
	}
	if status, ok := h.ended[runId]; ok {
		resultCh <- status
		return
	}
	status := domain.NewRunStatus()
	status.Status = domain.Status_UNKNOWN
	status.Info = "RunId not found"
	resultCh <- status
}

func (h *handler) doAbort(runId string, resultCh chan *domain.RunStatus) {
	if _, ok := h.running[runId]; ok {
		resultCh <- h.doAbortImpl(runId)
		return
	}
	if _, ok := h.ended[runId]; ok {
		status := domain.NewRunStatus()
		status.Status = domain.Status_INVALID
		status.Info = "Cannot abort an already ended run."
		resultCh <- status
		return
	}
	status := domain.NewRunStatus()
	status.Status = domain.Status_UNKNOWN
	status.Info = "RunId not found"
	resultCh <- status
}

func (h *handler) doClear(runId string, resultCh chan interface{}) {
	// This is optional bookkeeping and we either succeed or silently fail.
	delete(h.ended, runId)
}

// Loop handling rpc thunks and polling for the status of running jobs.
func (h *handler) loop() {
	const pollInterval time.Duration = time.Millisecond * time.Duration(250)
	pollTimer := time.After(pollInterval)
	for h.inCh != nil || len(h.running) > 0 {
		select {
		case thunk, ok := <-h.inCh:
			if !ok {
				h.inCh = nil
				break
			}
			thunk()
		case <-pollTimer:
			pollTimer = time.After(pollInterval)
			for runId, _ := range h.running {
				process, err := h.run.Status(runner.RunId(runId))
				if err != nil {
					panic(fmt.Sprintf("Runner doesn't know about runId: %s", runId))
				}
				h.running[runId] = processToRunStatus(&process, runId)
				if process.State.IsDone() {
					h.doCleanupRunImpl(runId)
				}
			}
			h.stat.Gauge("cachedEndedRuns").Update(int64(len(h.ended)))
		}
	}
}

// Implement thrift worker.Worker interface.
// Each rpc here sends a thunk to the main thread for execution and waits for a result.
//
func (h *handler) QueryWorker() (*worker.WorkerStatus, error) {
	h.stat.Counter("workerQueries").Inc(1)

	resultCh := make(chan *domain.WorkerStatus)
	h.inCh <- func() { h.doQueryWorker(resultCh) }
	return domain.DomainWorkerStatusToThrift(<-resultCh), nil
}

func (h *handler) Run(cmd *worker.RunCommand) (*worker.RunStatus, error) {
	defer h.stat.Latency("runLatency_ms").Time().Stop()
	h.stat.Counter("runs").Inc(1)

	log.Println("Running", render.Render(cmd))
	rc := domain.ThriftRunCommandToDomain(cmd)
	resultCh := make(chan *domain.RunStatus)
	h.inCh <- func() { h.doRun(rc, resultCh) }
	return domain.DomainRunStatusToThrift(<-resultCh), nil
}

func (h *handler) Query(runId string) (*worker.RunStatus, error) {
	h.stat.Counter("runQueries").Inc(1)

	resultCh := make(chan *domain.RunStatus)
	h.inCh <- func() { h.doQueryRun(runId, resultCh) }
	return domain.DomainRunStatusToThrift(<-resultCh), nil
}

func (h *handler) Abort(runId string) (*worker.RunStatus, error) {
	h.stat.Counter("aborts").Inc(1)

	resultCh := make(chan *domain.RunStatus)
	h.inCh <- func() { h.doAbort(runId, resultCh) }
	return domain.DomainRunStatusToThrift(<-resultCh), nil
}

func (h *handler) Clear(runId string) error {
	h.stat.Counter("clears").Inc(1)

	resultCh := make(chan interface{})
	h.inCh <- func() { h.doClear(runId, resultCh) }
	<-resultCh
	return nil
}
