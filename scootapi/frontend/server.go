package frontend

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

// Returns a new server Handler which is a frontend to the given remote cloud scoot.
func NewHandler(backingClient *scootapi.CloudScootClient, stat stats.StatsReceiver) scoot.CloudScoot {
	return &Handler{client: backingClient, stat: stat}
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

// Wrapping type that combines a frontend and stats. The thrift server delegates to this type.
type Handler struct {
	client *scootapi.CloudScootClient
	stat   stats.StatsReceiver
}

// Implements RunJob Cloud Scoot API
func (h *Handler) RunJob(def *scoot.JobDefinition) (*scoot.JobId, error) {
	defer h.stat.Latency("runJobLatency_ms").Time().Stop()
	h.stat.Counter("runJobRpmCounter").Inc(1)
	return h.client.RunJob(def)
}

// Implements GetStatus Cloud Scoot API
// TODO: do work here in frontend, don't delegate to remote cloud scoot.
func (h *Handler) GetStatus(jobId string) (*scoot.JobStatus, error) {
	defer h.stat.Latency("jobStatusLatency_ms").Time().Stop()
	h.stat.Counter("jobStatusRpmCounter").Inc(1)
	return h.client.GetStatus(jobId)
}
