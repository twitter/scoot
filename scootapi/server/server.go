package server

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched/scheduler"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
	"github.com/twitter/scoot/scootapi/server/api"
)

// Creates and returns a new server Handler, which combines the scheduler,
// saga coordinator and stats receivers.
func NewHandler(scheduler scheduler.Scheduler, sc saga.SagaCoordinator, stat stats.StatsReceiver) scoot.CloudScoot {
	handler := &Handler{scheduler: scheduler, sagaCoord: sc, stat: stat}
	go stats.StartUptimeReporting(stat, stats.SchedUptime_ms, stats.SchedServerStartedGauge, stats.DefaultStartupGaugeSpikeLen)
	return handler
}

// Creates a Thrift server given a Handler and Thrift connection information
func MakeServer(handler scoot.CloudScoot,
	transport thrift.TServerTransport,
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory) thrift.TServer {
	return thrift.NewTSimpleServer4(
		scoot.NewCloudScootProcessor(handler),
		transport, transportFactory, protocolFactory)
}

// Wrapping type that combines a scheduler, saga coordinator and stat receiver into a server
type Handler struct {
	scheduler scheduler.Scheduler
	sagaCoord saga.SagaCoordinator
	stat      stats.StatsReceiver
}

// Implements RunJob Cloud Scoot API
func (h *Handler) RunJob(def *scoot.JobDefinition) (*scoot.JobId, error) {
	defer h.stat.Latency(stats.SchedServerRunJobLatency_ms).Time().Stop() // TODO errata metric - remove if unused
	h.stat.Counter(stats.SchedServerRunJobCounter).Inc(1)                 // TODO errata metric - remove if unused
	return api.RunJob(h.scheduler, def, h.stat)
}

// Implements GetStatus Cloud Scoot API
func (h *Handler) GetStatus(jobId string) (*scoot.JobStatus, error) {
	defer h.stat.Latency(stats.SchedServerJobStatusLatency_ms).Time().Stop()
	h.stat.Counter(stats.SchedServerJobStatusCounter).Inc(1)
	return api.GetJobStatus(jobId, h.sagaCoord)
}

// Implements KillJob Cloud Scoot API
func (h *Handler) KillJob(jobId string) (*scoot.JobStatus, error) {
	defer h.stat.Latency(stats.SchedServerJobKillLatency_ms).Time().Stop()
	h.stat.Counter(stats.SchedServerJobKillCounter).Inc(1)
	return api.KillJob(jobId, h.scheduler, h.sagaCoord)
}

// Implements OfflineWorker Cloud Scoot API
func (h *Handler) OfflineWorker(req *scoot.OfflineWorkerReq) error {
	return api.OfflineWorker(req, h.scheduler)
}

// Implements ReinstateWorker Cloud Scoot API
func (h *Handler) ReinstateWorker(req *scoot.ReinstateWorkerReq) error {
	return api.ReinstateWorker(req, h.scheduler)
}
