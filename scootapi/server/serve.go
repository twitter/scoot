package server

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

// Creates and returns a new server Handler, which combines the scheduler,
// saga coordinator and stats receivers.
func NewHandler(scheduler scheduler.Scheduler, sc saga.SagaCoordinator, stat stats.StatsReceiver) scoot.CloudScoot {
	handler := &Handler{scheduler: scheduler, sagaCoord: sc, stat: stat}
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
	defer h.stat.Latency("runJobLatency_ms").Time().Stop()
	h.stat.Counter("runJobRpmCounter").Inc(1)
	return runJob(h.scheduler, def, h.stat)
}

// Implements GetStatus Cloud Scoot API
func (h *Handler) GetStatus(jobId string) (*scoot.JobStatus, error) {
	defer h.stat.Latency("jobStatusLatency_ms").Time().Stop()
	h.stat.Counter("jobStatusRpmCounter").Inc(1)
	return GetJobStatus(jobId, h.sagaCoord)
}
