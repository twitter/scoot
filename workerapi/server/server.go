package server

import (
	"fmt"
	"log"
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

type handler struct {
	stat            stats.StatsReceiver
	run             runner.Runner
	getVersion      func() string
	timeLastContact time.Time
}

func DefaultHandler(stat stats.StatsReceiver) worker.Worker {
	if stat == nil {
		stat = stats.NilStatsReceiver()
	}
	return NewHandler(
		stat,
		localrunner.NewSimpleRunner(execer.NewExecer()),
		func() string { return "" },
	)
}

func NewHandler(stat stats.StatsReceiver, run runner.Runner, getVersion func() string) worker.Worker {
	scopedStat := stat.Scope("handler")
	h := &handler{stat: scopedStat, run: run, getVersion: getVersion}
	go h.stats()
	return h
}

// Periodically output stats
func (h *handler) stats() {
	ticker := time.NewTicker(time.Millisecond * time.Duration(500))
	for {
		select {
		case <-ticker.C:
			var numFailed int64
			var numActive int64
			processes := h.run.StatusAll()
			for _, process := range processes {
				if process.State == runner.FAILED {
					numFailed++
				}
				if !process.State.IsDone() {
					numActive++
				}
			}
			failed := h.stat.Counter("numFailed")
			prevNumFailed := failed.Count()
			if numFailed > prevNumFailed {
				failed.Inc(numFailed - prevNumFailed)
			}
			h.stat.Gauge("numActiveRuns").Update(numActive)
			h.stat.Gauge("numEndedRuns").Update(int64(len(processes)) - numActive)
			h.stat.Gauge("timeSinceLastContact").Update(int64(time.Now().Sub(h.timeLastContact)))
		}
	}
}

// Implement thrift worker.Worker interface.
//
func (h *handler) QueryWorker() (*worker.WorkerStatus, error) {
	h.stat.Counter("workerQueries").Inc(1)
	h.timeLastContact = time.Now()
	ws := worker.NewWorkerStatus()
	version := h.getVersion()
	ws.VersionId = &version
	for _, process := range h.run.StatusAll() {
		ws.Runs = append(ws.Runs, domain.DomainRunStatusToThrift(&process))
	}
	return ws, nil
}

func (h *handler) Run(cmd *worker.RunCommand) (*worker.RunStatus, error) {
	defer h.stat.Latency("runLatency_ms").Time().Stop()
	h.stat.Counter("runs").Inc(1)
	log.Println("Running", render.Render(cmd))

	h.timeLastContact = time.Now()
	process := h.run.Run(domain.ThriftRunCommandToDomain(cmd))
	return domain.DomainRunStatusToThrift(&process), nil
}

func (h *handler) Abort(runId string) (*worker.RunStatus, error) {
	h.stat.Counter("aborts").Inc(1)
	h.timeLastContact = time.Now()
	process := h.run.Abort(runner.RunId(runId))
	return domain.DomainRunStatusToThrift(&process), nil
}

func (h *handler) Erase(runId string) error {
	h.stat.Counter("clears").Inc(1)
	h.timeLastContact = time.Now()
	h.run.Erase(runner.RunId(runId))
	return nil
}
