package server

import (
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

func NewHandler(scheduler scheduler.Scheduler, sc saga.SagaCoordinator, stat stats.StatsReceiver) scoot.CloudScoot {
	handler := &Handler{scheduler: scheduler, sagaCoord: sc, stat: stat}
	handler.loop()
	return handler
}

func MakeServer(handler scoot.CloudScoot,
	transport thrift.TServerTransport, transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) thrift.TServer {
	return thrift.NewTSimpleServer4(
		scoot.NewCloudScootProcessor(handler),
		transport, transportFactory, protocolFactory)
}

type Handler struct {
	scheduler          scheduler.Scheduler
	sagaCoord          saga.SagaCoordinator
	stat               stats.StatsReceiver
	runJobStatCount    int64
	jobStatusStatCount int64
}

func (h *Handler) loop() {
	tickerS := time.NewTicker(time.Second)
	tickerM := time.NewTicker(time.Minute)
	for {
		// Manually latch and reset until we find a better way to do req/sec within viz's one minute window.
		// Takes max rps for every one minute window.
		select {
		case <-tickerS.C:
			prevRunRps := h.stat.Gauge("runJobRpsGauge").Value()
			prevStatusRps := h.stat.Gauge("jobStatusRpsGauge").Value()
			if prevRunRps < h.runJobStatCount {
				h.stat.Gauge("runJobRpsGauge").Update(h.runJobStatCount)
			}
			if prevStatusRps < h.jobStatusStatCount {
				h.stat.Gauge("jobStatusRpsGauge").Update(h.jobStatusStatCount)
			}
		case <-tickerM.C:
			h.stat.Gauge("runJobRpsGauge").Update(0)
			h.runJobStatCount = 0
			h.jobStatusStatCount = 0
		}
	}
}

func (h *Handler) RunJob(def *scoot.JobDefinition) (*scoot.JobId, error) {
	defer h.stat.Latency("runJobLatency_ms").Time().Stop()
	h.runJobStatCount++
	return runJob(h.scheduler, def, h.stat)
}

func (h *Handler) GetStatus(jobId string) (*scoot.JobStatus, error) {
	defer h.stat.Latency("jobStatusLatency_ms").Time().Stop()
	h.jobStatusStatCount++
	return GetJobStatus(jobId, h.sagaCoord)
}
