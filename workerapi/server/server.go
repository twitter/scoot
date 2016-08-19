package server

import (
	"fmt"
	"log"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/runner"
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
	stat        stats.StatsReceiver
	run         runner.Runner
	getVersion  func() string
	timeLastRpc time.Time
}

func NewHandler(stat stats.StatsReceiver, run runner.Runner, getVersion func() string) worker.Worker {
	scopedStat := stat.Scope("handler")
	h := &handler{stat: scopedStat, run: run, getVersion: getVersion}
	go h.stats()
	return h
}

// Periodically output stats
//TODO: runner should eventually be extended to support stats, multiple runs, etc. (replacing loop here).
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
			lastContact := int64(time.Now().Sub(h.timeLastRpc) / time.Millisecond)
			failed := h.stat.Counter("numFailed")
			prevNumFailed := failed.Count()
			if numFailed > prevNumFailed {
				failed.Inc(numFailed - prevNumFailed)
			}
			h.stat.Gauge("numActiveRuns").Update(numActive)
			h.stat.Gauge("numEndedRuns").Update(int64(len(processes)) - numActive)
			h.stat.Gauge("timeSinceLastContact_ms").Update(lastContact)
		}
	}
}

// Implement thrift worker.Worker interface.
//
func (h *handler) QueryWorker() (*worker.WorkerStatus, error) {
	h.stat.Counter("workerQueries").Inc(1)
	h.timeLastRpc = time.Now()
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
	log.Println("WorkerRunning", render.Render(cmd))

	h.timeLastRpc = time.Now()
	process := h.run.Run(domain.ThriftRunCommandToDomain(cmd))
	return domain.DomainRunStatusToThrift(&process), nil
}

func (h *handler) Abort(runId string) (*worker.RunStatus, error) {
	h.stat.Counter("aborts").Inc(1)
	h.timeLastRpc = time.Now()
	process := h.run.Abort(runner.RunId(runId))
	return domain.DomainRunStatusToThrift(&process), nil
}

func (h *handler) Erase(runId string) error {
	h.stat.Counter("clears").Inc(1)
	h.timeLastRpc = time.Now()
	h.run.Erase(runner.RunId(runId))
	return nil
}
