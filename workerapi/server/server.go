// Package server provides the implementation of the Scoot Worker
// Server, which implements the Worker API and starts the actual worker.
package server

import (
	"reflect"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/runners"
	domain "github.com/scootdev/scoot/workerapi"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

// Creates a Worker Server
func MakeServer(
	handler worker.Worker,
	transport thrift.TServerTransport,
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory) thrift.TServer {
	return thrift.NewTSimpleServer4(
		worker.NewWorkerProcessor(handler),
		transport,
		transportFactory,
		protocolFactory)
}

type handler struct {
	stat         stats.StatsReceiver
	run          runner.Service
	timeLastRpc  time.Time
	mu           sync.Mutex
	currentCmd   *runner.Command
	currentRunID runner.RunID
}

// Creates a new Handler which combines a runner.Service to do work and a StatsReceiver
func NewHandler(stat stats.StatsReceiver, run runner.Service) worker.Worker {
	scopedStat := stat.Scope("handler")
	h := &handler{stat: scopedStat, run: run}
	go h.stats()
	return h
}

// Periodically output stats
//TODO: runner should eventually be extended to support stats, multiple runs, etc. (replacing loop here).
func (h *handler) stats() {
	var startTime time.Time
	nilTime := time.Time{}
	ticker := time.NewTicker(time.Millisecond * time.Duration(500))
	for {
		select {
		case <-ticker.C:
			h.mu.Lock()
			var numFailed int64
			var numActive int64
			processes, svc, err := h.run.StatusAll()
			if err != nil {
				continue
			}
			if svc.Initialized && startTime == nilTime {
				startTime = time.Now()
			}
			for _, process := range processes {
				if process.State == runner.FAILED {
					numFailed++
				}
				if !process.State.IsDone() {
					numActive++
				}
			}
			timeSinceLastContact_ms := int64(0)
			if numActive > 0 {
				timeSinceLastContact_ms = int64(time.Now().Sub(h.timeLastRpc) / time.Millisecond)
			}
			var uptime time.Duration
			if startTime != nilTime {
				uptime = time.Since(startTime)
			}
			h.stat.Gauge("activeRunsGauge").Update(numActive)
			h.stat.Gauge("failedCachedRunsGauge").Update(numFailed)
			h.stat.Gauge("endedCachedRunsGauge").Update(int64(len(processes)) - numActive)
			h.stat.Gauge("timeSinceLastContactGauge_ms").Update(timeSinceLastContact_ms)
			h.stat.Gauge("uptimeGauge_ms").Update(int64(uptime / time.Millisecond))
			h.mu.Unlock()
		}
	}
}

// Convenience
func (h *handler) updateTimeLastRpc() {
	h.mu.Lock()
	h.timeLastRpc = time.Now()
	h.mu.Unlock()
}

// Implements worker.thrift Worker.QueryWorker interface
func (h *handler) QueryWorker() (*worker.WorkerStatus, error) {
	h.stat.Counter("workerQueries").Inc(1)
	h.updateTimeLastRpc()
	ws := worker.NewWorkerStatus()

	st, svc, err := h.run.StatusAll()
	if err != nil {
		ws.Error = err.Error()
	}
	ws.Initialized = svc.Initialized

	for _, status := range st {
		if status.State.IsDone() {
			// Note: TravisCI fails when output is too long so we set full status to Debug and disable it when running in that env.
			log.Infof("Worker returning finished run: %v", status.RunID)
			log.Debugf("Worker returning finished run (details): %v", status)
		}
		ws.Runs = append(ws.Runs, domain.DomainRunStatusToThrift(status))
	}
	return ws, nil
}

// Implements worker.thrift Worker.Run interface
func (h *handler) Run(cmd *worker.RunCommand) (*worker.RunStatus, error) {
	defer h.stat.Latency("runLatency_ms").Time().Stop()
	h.stat.Counter("runs").Inc(1)
	log.Infof("Worker trying to run cmd: %v", cmd)

	h.updateTimeLastRpc()
	c := domain.ThriftRunCommandToDomain(cmd)
	status, err := h.run.Run(c)
	//Check if this is a dup retry for an already running command and if so get its status.
	//TODO(jschiller): accept a cmd.Nonce field so we can be precise about hiccups with dup cmd resends?
	if err != nil && err.Error() == runners.QueueFullMsg && reflect.DeepEqual(c, h.currentCmd) {
		log.Infof("Worker received dup request, recovering runID: %v", h.currentRunID)
		status, _, err = h.run.Status(h.currentRunID)
	}
	if err != nil {
		// Set invalid status and nil err to indicate handleable domain err.
		status.Error = err.Error()
		status.State = runner.BADREQUEST
	} else {
		h.currentCmd = c
		h.currentRunID = status.RunID
	}
	log.Infof("Worker returning run status: %v", status)
	return domain.DomainRunStatusToThrift(status), nil
}

// Implements worker.thrift Worker.Abort interface
func (h *handler) Abort(runId string) (*worker.RunStatus, error) {
	h.stat.Counter("aborts").Inc(1)
	h.updateTimeLastRpc()
	log.Infof("Worker aborting runID: %s", runId)
	status, err := h.run.Abort(runner.RunID(runId))
	if err != nil {
		// Set invalid status and nil err to indicate handleable domain err.
		status.Error = err.Error()
		status.State = runner.UNKNOWN
		status.RunID = runner.RunID(runId)
	}
	return domain.DomainRunStatusToThrift(status), nil
}

// Implements worker.thrift Worker.Erase interface
func (h *handler) Erase(runId string) error {
	h.stat.Counter("clears").Inc(1)
	h.updateTimeLastRpc()
	log.Infof("Worker erasing runID: %s", runId)
	h.run.Erase(runner.RunID(runId))
	return nil
}
