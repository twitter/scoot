package api

// these functions are the service side entry points for the thrift protocol
// (they are call from cloudscoot.go)

import (
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/wisechengyi/scoot/common/stats"
	"github.com/wisechengyi/scoot/saga"
	schedthrift "github.com/wisechengyi/scoot/scheduler/api/thrift"
	"github.com/wisechengyi/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/wisechengyi/scoot/scheduler/server"
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

// Implements SetSchedulerStatus Cloud Scoot API
func (h *Handler) SetSchedulerStatus(maxNumTasks int32) error {
	return schedthrift.SetSchedulerStatus(h.scheduler, maxNumTasks)
}

// GetClassLoadPercents Implements GetClassLoadPercents Cloud Scoot API
func (h *Handler) GetClassLoadPercents() (map[string]int32, error) {
	clp, err := schedthrift.GetClassLoadPercents(h.scheduler)
	log.Infof("GetClassLoadPercents returning: %v, err:%v", clp, err)
	return clp, err
}

// SetClassLoadPercents Implements SetClassLoadPercents Cloud Scoot API
func (h *Handler) SetClassLoadPercents(classLoadPercents map[string]int32) error {
	log.Infof("SetClassLoadPercents to %v", classLoadPercents)
	return schedthrift.SetClassLoadPercents(h.scheduler, classLoadPercents)
}

// GetRequestorToClassMap Implements GetRequestorToClassMap Cloud Scoot API
func (h *Handler) GetRequestorToClassMap() (map[string]string, error) {
	rm, err := schedthrift.GetRequestorToClassMap(h.scheduler)
	log.Infof("GetClassLoadPercents returning: %v, err:%v", rm, err)
	return rm, err
}

// SetRequestorToClassMap Implements SetRequestorToClassMap Cloud Scoot API
func (h *Handler) SetRequestorToClassMap(requestToClassMap map[string]string) error {
	log.Infof("SetRequestorToClassMap to %v", requestToClassMap)
	return schedthrift.SetRequestorToClassMap(h.scheduler, requestToClassMap)
}

// GetRebalanceMinimumDuration get the duration(minutes) that the scheduler needs to be exceeding
// the rebalance threshold before rebalancing.  <= 0 implies no rebalancing
func (h *Handler) GetRebalanceMinimumDuration() (int32, error) {
	d, err := schedthrift.GetRebalanceMinimumDuration(h.scheduler)
	log.Infof("GetRebalanceMinimumDuration returning: %d, err:%v", d, err)
	return int32(d.Minutes()), err
}

// SetRebalanceMinimumDuration set the duration(minutes) that the scheduler needs to be exceeding
// the rebalance threshold before rebalancing.  <= 0 implies no rebalancing
func (h *Handler) SetRebalanceMinimumDuration(durationMinimum int32) error {
	log.Infof("SetRebalanceMinimumDuration to %d", durationMinimum)
	d := time.Duration(durationMinimum) * time.Minute
	return schedthrift.SetRebalanceMinimumDuration(h.scheduler, d)
}

// GetRebalanceThreshold the % spread threshold that must be exceeded to trigger rebalance
// <= 0 implies no rebalancing
func (h *Handler) GetRebalanceThreshold() (int32, error) {
	t, err := schedthrift.GetRebalanceThreshold(h.scheduler)
	log.Infof("GetRebalanceThreshold returning: %d, err:%v", t, err)
	return t, err
}

// SetRebalanceThreshold the % spread threshold that must be exceeded to trigger rebalance
// <= 0 implies no rebalancing
func (h *Handler) SetRebalanceThreshold(threshold int32) error {
	log.Infof("SetRebalanceThreshold to %d", threshold)
	return schedthrift.SetRebalanceThreshold(h.scheduler, threshold)
}
