package server

import (
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

func NewHandler(scheduler scheduler.Scheduler, sc saga.SagaCoordinator, stat stats.StatsReceiver) scoot.CloudScoot {
	handler := &Handler{scheduler: scheduler, sagaCoord: sc, stat: stat}
	go handler.loop()
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
	mu                 sync.Mutex
}

func (h *Handler) loop() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			h.mu.Lock()
			h.stat.Gauge("runJobRpmGauge").Update(h.runJobStatCount)
			h.stat.Gauge("jobStatusRpmGauge").Update(h.jobStatusStatCount)
			h.runJobStatCount = 0
			h.jobStatusStatCount = 0
			h.mu.Unlock()
		}
	}
}

func (h *Handler) RunJob(def *scoot.JobDefinition) (*scoot.JobId, error) {
	defer h.stat.Latency("runJobLatency_ms").Time().Stop()
	h.mu.Lock()
	h.runJobStatCount++
	h.mu.Unlock()
	return runJob(h.scheduler, def, h.stat)
}

func (h *Handler) GetStatus(jobId string) (*scoot.JobStatus, error) {
	defer h.stat.Latency("jobStatusLatency_ms").Time().Stop()
	h.mu.Lock()
	h.jobStatusStatCount++
	h.mu.Unlock()
	return GetJobStatus(jobId, h.sagaCoord)
}
