package server

import (
	"fmt"
	"log"
	"time"

	thriftlib "github.com/apache/thrift/lib/go/thrift"
	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/runner"
	domain "github.com/scootdev/scoot/workerapi"
	thrift "github.com/scootdev/scoot/workerapi/gen-go/worker"
)

// Called by a main binary. Blocks until the connection is terminated.
func Serve(handler thrift.Worker, addr string, transportFactory thriftlib.TTransportFactory, protocolFactory thriftlib.TProtocolFactory) error {
	transport, err := thriftlib.NewTServerSocket(addr)
	if err != nil {
		return err
	}
	processor := thrift.NewWorkerProcessor(handler)
	server := thriftlib.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	fmt.Println("Serving thrift: ", addr)

	return server.Serve()
}

type handler struct {
	stat        stats.StatsReceiver
	worker      domain.Worker
	getVersion  func() string
	timeLastRpc time.Time
}

func NewHandler(stat stats.StatsReceiver, worker domain.Worker, getVersion func() string) thrift.Worker {
	h := &handler{
		stat:       stat.Scope("handler"),
		worker:     worker,
		getVersion: getVersion,
	}
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
			ws, err := h.worker.Status()
			if err != nil {
				return
			}
			for _, process := range ws.Runs {
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
			h.stat.Gauge("numEndedRuns").Update(int64(len(ws.Runs)) - numActive)
			h.stat.Gauge("timeSinceLastContact").Update(int64(time.Now().Sub(h.timeLastRpc)))
		}
	}
}

// Implement thrift worker.Worker interface.
//
func (h *handler) QueryWorker() (*thrift.WorkerStatus, error) {
	h.stat.Counter("workerQueries").Inc(1)
	h.timeLastRpc = time.Now()
	status, err := h.worker.Status()
	return domain.DomainWorkerStatusToThrift(status), err
}

func (h *handler) Run(cmd *thrift.RunCommand) (*thrift.RunStatus, error) {
	defer h.stat.Latency("runLatency_ms").Time().Stop()
	h.stat.Counter("runs").Inc(1)
	log.Println("Running", render.Render(cmd))

	h.timeLastRpc = time.Now()
	h.stat.Gauge("timeSinceLastContact").Update(int64(time.Now().Sub(h.timeLastRpc)))
	process, err := h.worker.Run(domain.ThriftRunCommandToDomain(cmd))
	return domain.DomainRunStatusToThrift(process), err
}

func (h *handler) Abort(runId string) (*thrift.RunStatus, error) {
	h.stat.Counter("aborts").Inc(1)
	h.timeLastRpc = time.Now()
	process, err := h.worker.Abort(runner.RunId(runId))
	return domain.DomainRunStatusToThrift(process), err
}

func (h *handler) Erase(runId string) error {
	h.stat.Counter("clears").Inc(1)
	h.timeLastRpc = time.Now()
	h.worker.Erase(runner.RunId(runId))
	return nil
}
