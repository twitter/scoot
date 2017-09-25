// Package server provides the implementation of the Scoot Worker
// Server, which implements the Worker API and starts the actual worker.
package server

import (
	"reflect"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/helpers"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/runners"
	domain "github.com/twitter/scoot/workerapi"
	"github.com/twitter/scoot/workerapi/gen-go/worker"
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

// defining StatsCollectInterval type so it can be injected via ICE
// (avoid conflict with other injected integers)
type StatsCollectInterval time.Duration

type handler struct {
	stat         stats.StatsReceiver
	run          runner.Service
	timeLastRpc  time.Time
	mu           sync.RWMutex
	currentCmd   *runner.Command
	currentRunID runner.RunID
}

// Creates a new Handler which combines a runner.Service to do work and a StatsReceiver
func NewHandler(stat stats.StatsReceiver, run runner.Service) worker.Worker {
	scopedStat := stat.Scope("handler")
	h := &handler{stat: scopedStat, run: run, timeLastRpc: time.Now()}
	stats.ReportServerRestart(scopedStat, stats.WorkerServerStartedGauge, stats.DefaultStartupGaugeSpikeLen)
	go h.stats()
	return h
}

// Periodically output stats
//TODO: runner should eventually be extended to support stats, multiple runs, etc. (replacing loop here).
func (h *handler) stats() {
	var startTime time.Time = time.Now()
	var initTime time.Duration
	nilTime := time.Time{}
	initDoneTime := nilTime
	ticker := time.NewTicker(time.Duration(stats.StatReportIntvl))
	for {
		select {
		case <-ticker.C:
			h.mu.Lock()

			processes, svcStatus, err := h.run.StatusAll()
			if err != nil {
				continue
			}

			if svcStatus.Initialized {
				if initDoneTime == nilTime {
					initDoneTime = time.Now()
					initTime = initDoneTime.Sub(startTime)
				}

				var numFailed int64
				var numActive int64

				for _, process := range processes {
					if process.State == runner.FAILED {
						numFailed++
					}
					if !process.State.IsDone() {
						numActive++
					}
				}
				// if its done initializing, record the final initLatency time
				// compute the uptime as time since init finished
				uptime := time.Since(initDoneTime)
				timeSincelastContact_ms := int64(time.Now().Sub(h.timeLastRpc) / time.Millisecond)
				h.stat.Gauge(stats.WorkerFinalInitLatency_ms).Update(int64(initTime / time.Millisecond))
				h.stat.Gauge(stats.WorkerActiveInitLatency_ms).Update(0)
				h.stat.Gauge(stats.WorkerActiveRunsGauge).Update(numActive)
				h.stat.Gauge(stats.WorkerFailedCachedRunsGauge).Update(numFailed)
				h.stat.Gauge(stats.WorkerEndedCachedRunsGauge).Update(int64(len(processes)) - numActive) // TODO errata metric - remove if unused
				h.stat.Gauge(stats.WorkerTimeSinceLastContactGauge_ms).Update(timeSincelastContact_ms)   // TODO errata metric - remove if unused
				uptimeMs := int64(uptime / time.Millisecond)
				h.stat.Gauge(stats.WorkerUptimeGauge_ms).Update(uptimeMs)
			} else {
				initTime := time.Now().Sub(startTime)
				h.stat.Gauge(stats.WorkerActiveInitLatency_ms).Update(int64(initTime / time.Millisecond))
			}

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
	h.stat.Counter(stats.WorkerServerQueries).Inc(1)
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
			if log.GetLevel() == log.DebugLevel {
				log.Debugf("Worker returning finished run: %v", status)
			} else {
				log.Infof("Worker returning finished run: %v", status.RunID)
			}
		}
		ws.Runs = append(ws.Runs, domain.DomainRunStatusToThrift(status))
	}
	return ws, nil
}

// Implements worker.thrift Worker.Run interface
func (h *handler) Run(cmd *worker.RunCommand) (*worker.RunStatus, error) {
	defer h.stat.Latency(stats.WorkerServerStartRunLatency_ms).Time().Stop()
	h.stat.Counter(stats.WorkerServerRuns).Inc(1)
	log.WithFields(
		log.Fields{
			"argv":       cmd.Argv,
			"env":        cmd.Env,
			"snapshotId": helpers.CopyPointerToString(cmd.SnapshotId),
			"timeoutMs":  helpers.CopyPointerToInt32(cmd.TimeoutMs),
			"jobID":      helpers.CopyPointerToString(cmd.JobId),
			"taskID":     helpers.CopyPointerToString(cmd.TaskId),
			"tag":        helpers.CopyPointerToString(cmd.Tag),
		}).Info("Worker trying to run cmd")

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
		// Set invalid status and nil err to indicate handleable internal err.
		status.Error = err.Error()
		status.State = runner.BADREQUEST
	} else {
		h.currentCmd = c
		h.currentRunID = status.RunID
	}
	// status's stdout, stderr, taskID, jobID, and tag might not be populated yet.
	// h.run.Run(c) calls *runner.Invoker#run in a goroutine, and these fields are set on the fly
	log.WithFields(
		log.Fields{
			"runID":      status.RunID,
			"snapshotID": status.SnapshotID,
			"state":      status.State,
			"jobID":      status.JobID,
			"taskID":     status.TaskID,
			"tag":        status.Tag,
			"stdout":     status.StdoutRef,
			"stderr":     status.StderrRef,
			"error":      status.Error,
		}).Info("Worker returning run status")
	return domain.DomainRunStatusToThrift(status), nil
}

// Implements worker.thrift Worker.Abort interface
func (h *handler) Abort(runId string) (*worker.RunStatus, error) {
	h.stat.Counter(stats.WorkerServerAborts).Inc(1)
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
	h.stat.Counter(stats.WorkerServerClears).Inc(1)
	h.updateTimeLastRpc()
	log.Infof("Worker erasing runID: %s", runId)
	h.run.Erase(runner.RunID(runId))
	return nil
}
