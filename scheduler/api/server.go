package api

// these functions are the service side entry points for the thrift protocol
// (they are call from cloudscoot.go)

import (
	"github.com/apache/thrift/lib/go/thrift"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/saga"
	schedthrift "github.com/twitter/scoot/scheduler/api/thrift"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/twitter/scoot/scheduler/server"
)

// Creates and returns a new server Handler, which combines the scheduler,
// saga coordinator and stats receivers.
func NewHandler(scheduler server.Scheduler, sc saga.SagaCoordinator, stat stats.StatsReceiver) scoot.CloudScoot {
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
	scheduler server.Scheduler
	sagaCoord saga.SagaCoordinator
	stat      stats.StatsReceiver
}

// Implements RunJob Cloud Scoot API
func (h *Handler) RunJob(def *scoot.JobDefinition) (*scoot.JobId, error) {
	defer h.stat.Latency(stats.SchedServerRunJobLatency_ms).Time().Stop() // TODO errata metric - remove if unused
	h.stat.Counter(stats.SchedServerRunJobCounter).Inc(1)                 // TODO errata metric - remove if unused
	return schedthrift.RunJob(h.scheduler, def, h.stat)
}

// Implements GetStatus Cloud Scoot API
func (h *Handler) GetStatus(jobId string) (*scoot.JobStatus, error) {
	defer h.stat.Latency(stats.SchedServerJobStatusLatency_ms).Time().Stop()
	h.stat.Counter(stats.SchedServerJobStatusCounter).Inc(1)
	return schedthrift.GetJobStatus(jobId, h.sagaCoord)
}

// Implements KillJob Cloud Scoot API
func (h *Handler) KillJob(jobId string) (*scoot.JobStatus, error) {
	defer h.stat.Latency(stats.SchedServerJobKillLatency_ms).Time().Stop()
	h.stat.Counter(stats.SchedServerJobKillCounter).Inc(1)
	return schedthrift.KillJob(jobId, h.scheduler, h.sagaCoord)
}

// Implements OfflineWorker Cloud Scoot API
func (h *Handler) OfflineWorker(req *scoot.OfflineWorkerReq) error {
	return schedthrift.OfflineWorker(req, h.scheduler)
}

// Implements ReinstateWorker Cloud Scoot API
func (h *Handler) ReinstateWorker(req *scoot.ReinstateWorkerReq) error {
	return schedthrift.ReinstateWorker(req, h.scheduler)
}

// Implements GetSchedulerStatus Cloud Scoot API
func (h *Handler) GetSchedulerStatus() (*scoot.SchedulerStatus, error) {
	return schedthrift.GetSchedulerStatus(h.scheduler)
}

// SetSchedulerStatus  Implements SetSchedulerStatus Cloud Scoot API
func (h *Handler) SetSchedulerStatus(maxNumTasks int32) error {
	return schedthrift.SetSchedulerStatus(h.scheduler, maxNumTasks)
}

// GetClassLoadPcts Implements GetClassLoadPcts Cloud Scoot API
func (h *Handler) GetClassLoadPcts() (map[string]int32, error) {
	return schedthrift.GetClassLoadPcts(h.scheduler)
}

// SetClassLoadPcts Implements SetClassLoadPcts Cloud Scoot API
func (h *Handler) SetClassLoadPcts(classLoadPcts map[string]int32) error {
	return schedthrift.SetClassLoadPcts(h.scheduler, classLoadPcts)
}

// GetRequestorToClassMap Implements GetRequestorToClassMap Cloud Scoot API
func (h *Handler) GetRequestorToClassMap() (map[string]string, error) {
	return schedthrift.GetRequestorToClassMap(h.scheduler)
}

// SetRequestorToClassMap Implements SetRequestorToClassMap Cloud Scoot API
func (h *Handler) SetRequestorToClassMap(requestToClassMap map[string]string) error {
	return schedthrift.SetRequestorToClassMap(h.scheduler, requestToClassMap)

}
