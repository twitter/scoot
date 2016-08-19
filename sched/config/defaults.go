package config

import (
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/cloud/cluster"
	clusterimpl "github.com/scootdev/scoot/cloud/cluster/memory"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/queue"
	queueimpl "github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/sched/worker/fake"
	"github.com/scootdev/scoot/sched/worker/rpc"
)

func DefaultParser() *Parser {
	r := &Parser{
		Cluster: map[string]ClusterConfig{
			"memory": &ClusterMemoryConfig{},
			"static": &ClusterStaticConfig{},
			"":       &ClusterMemoryConfig{Type: "memory", Count: 10},
		},
		Queue: map[string]QueueConfig{
			"memory": &QueueMemoryConfig{},
			"":       &QueueMemoryConfig{Type: "memory", Capacity: 1000},
		},
		SagaLog: map[string]SagaLogConfig{
			"memory": &SagaLogMemoryConfig{},
			"":       &SagaLogMemoryConfig{},
		},
		Workers: map[string]WorkersConfig{
			"local": &LocalWorkersConfig{},
			"rpc":   &RPCWorkersConfig{},
			"":      &LocalWorkersConfig{Type: "local"},
		},
		Report: map[string]ReportConfig{
			"default": &DefaultReportConfig{},
			"": &DefaultReportConfig{
				Type:       "default",
				StatsPath:  "/admin/metrics.json",
				HealthPath: "/",
				HttpAddr:   "localhost:9091",
			},
		},
	}
	return r
}

type ClusterMemoryConfig struct {
	Type  string
	Count int
}

func (c *ClusterMemoryConfig) Create() (cluster.Cluster, error) {
	workerNodes := []cluster.Node{}
	for i := 0; i < c.Count; i++ {
		workerNodes = append(workerNodes, clusterimpl.NewIdNode(fmt.Sprintf("inmemory%d", i)))
	}
	return clusterimpl.NewCluster(workerNodes, nil), nil
}

type ClusterStaticConfig struct {
	Type  string
	Hosts []string
}

func (c *ClusterStaticConfig) Create() (cluster.Cluster, error) {
	workerNodes := []cluster.Node{}
	for _, h := range c.Hosts {
		workerNodes = append(workerNodes, clusterimpl.NewIdNode(h))
	}
	return clusterimpl.NewCluster(workerNodes, nil), nil
}

type QueueMemoryConfig struct {
	Type     string
	Capacity int
}

func (c *QueueMemoryConfig) Create() (queue.Queue, error) {
	return queueimpl.NewSimpleQueue(c.Capacity), nil
}

type SagaLogMemoryConfig struct {
	Type string
}

func (c *SagaLogMemoryConfig) Create() (saga.SagaLog, error) {
	return saga.MakeInMemorySagaLog(), nil
}

type LocalWorkersConfig struct {
	Type string
	// TODO(dbentley): allow specifying what the runner/execer underneath this local worker is like
}

func (c *LocalWorkersConfig) Create() (worker.WorkerFactory, error) {
	return fake.MakeWaitingNoopWorker, nil
}

type RPCWorkersConfig struct {
	Type string
}

func (c *RPCWorkersConfig) Create() (worker.WorkerFactory, error) {
	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	return func(node cluster.Node) worker.Worker {
		return rpc.NewThriftWorker(transportFactory, protocolFactory, string(node.Id()))
	}, nil
}

type DefaultReportConfig struct {
	Type       string
	StatsPath  string
	HealthPath string
	QuitPath   string
	AbortPath  string
	HttpAddr   string
}

func (c *DefaultReportConfig) Create() (stat stats.StatsReceiver, err error) {
	stat = stats.NilStatsReceiver()
	if c.StatsPath != "" {
		stat, _ = stats.NewCustomStatsReceiver(stats.NewFinagleStatsRegistry, 15*time.Second)
		stat = stat.Precision(time.Millisecond)
		endpoints.RegisterStats("/admin/metrics.json", stat)
	}
	if c.HealthPath != "" {
		endpoints.RegisterHealthCheck("/")
	}

	//TODO(jschiller): quit and abort endpoints.

	go endpoints.Serve(c.HttpAddr)
	return
}
