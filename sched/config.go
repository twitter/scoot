package sched

import (
	"encoding/json"
	"fmt"
	"ioutil"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	clusterdef "github.com/scootdev/scoot/cloud/cluster"
	clusterimpl "github.com/scootdev/scoot/cloud/cluster/memory"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/distributor"
	"github.com/scootdev/scoot/sched/queue"
	queueimpl "github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/sched/worker"
	//"github.com/scootdev/scoot/sched/worker"

	"github.com/scootdev/scoot/sched/worker/fake"
	"github.com/scootdev/scoot/sched/worker/rpc"
	// "github.com/scootdev/scoot/sched/worker/rpc"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/scootapi/server"
)

//
//TODO: handle all config values. handle errors. decide extensibility story.
//

func (c *Config) init() {
	if c.ClusterImpl == nil {
		nodes, _ := c.CreateNodes()
		c.ClusterImpl, _ = c.CreateCluster(nodes)
	}
	if c.CoordImpl == nil {
		c.CoordImpl, _ = c.CreateSagaCoordinator(c.ClusterImpl)
	}
	if c.DistImpl == nil {
		c.DistImpl, _ = c.CreateDistributor(c.ClusterImpl)
	}
	if c.WorkerFactory == nil {
		c.WorkerFactory, _ = c.CreateWorkerFactory()
	}
	if c.QueueImpl == nil {
		c.QueueImpl, _ = c.CreateQueue()
	}
	c.initialized = true
}

//
func (c *Config) ToScheduler() (Scheduler, error) {
	if !c.initialized {
		c.Init()
	}
	return scheduler.NewScheduler(c.DistImpl, c.CoordImpl, c.WorkerFactory), nil
}

//
func (c *Config) ToServerHandler() (scoot.CloudScoot, error) {
	if !c.initialized {
		c.Init()
	}
	handler := server.NewHandler(c.QueueImpl, c.CoordImpl)
	return handler, nil
}

//
func (c *Config) CreateNodes() ([]clusterdef.Node, error) {
	// Create the cluster nodes.
	workerNodes := []clusterdef.Node{}
	switch c.Cluster.Type {
	case ClusterMemory:
		for idx := 0; idx < c.Cluster.Count; idx++ {
			workerNodes = append(workerNodes, clusterImpl.NewIdNode(fmt.Sprintf("inmemory%d", idx)))
		}
	case ClusterStatic:
		for idx := 0; idx < c.Cluster.Hosts; idx++ {
			workerNodes = append(workerNodes, clusterImpl.NewIdNode(cluster.Hosts[idx]))
		}
	case ClusterDynamic:
		//TODO
	}
	return workerNodes, nil
}

//
func (c *Config) CreateCluster(workerNodes []clusterdef.Node) (clusterdef.Cluster, error) {
	// TODO: replace with actual cluster implementation, currently dummy in memory cluster
	cluster := clusterimpl.NewCluster(workerNodes, nil)
	return cluster, nil
}

//
func (c *Config) CreateDistributor(cluster clusterdef.Cluster) (*distributor.PoolDistributor, error) {
	dist, err := distributor.NewPoolDistributorFromCluster(cluster)
	if err != nil {
		log.Fatalf("Error subscribing to cluster: %v", err)
	}
	return dist, nil
}

//
func (c *Config) CreateSagaCoordinator(cluster clusterdef.Cluster) (*saga.SagaCoordinator, error) {
	// TODO: Replace with Durable SagaLog, currently In Memory Only
	sagaCoordinator := saga.MakeInMemorySagaCoordinator()
	return saga, nil
}

//
func (c *Config) CreateWorkerFactory() worker.Worker {
	workerFactory := fake.MakeWaitingNoopWorker
	if c.Cluster.Type != ClusterMemory {
		workerFactory = func(node clusterdef.Node) worker.Worker {
			return rpc.NewThriftWorker(c.TransportFactory, c.ProtocolFactory, string(node.Id()))
		}
	}
	return workerFactory, nil
}

//
func (c *Config) CreateQueue() (queue.Queue, error) {
	// TODO: Replace with Durable WorkQueue, currently in Memory Only
	workQueue := queueimpl.NewSimpleQueue(*c.Queue.Capacity)
	return workQueue, nil
}

//
//
func NewSchedConfig(cfgFile string) (*Config, error) {
	cfg := &SchedConfig{
		ProtocolFactory:  thrift.NewTBinaryProtocolFactoryDefault(),
		TransportFactory: thrift.NewTTransportFactory(),

		Cluster: ClusterConfig{
			Type:    ClusterMemory,
			Initial: 5,
			Count:   IntToPtr(10),
		},
		Queue: QueueConfig{
			Type:     QueueMemory,
			Capacity: IntToPtr(1000),
		},
		SagaLog: SagaLogConfig{
			Type: SagaLogMemory,
		},
		Worker: WorkerConfig{
			Type: WorkerMemory,
			Runner: RunnerConfig{
				Type: RunnerSimple,
				Exec: ExecConfig{
					Type: ExecOs,
				},
			},
		},
		Planner: PlannerConfig{
			Type: PlannerSimple,
		},
		Stats: StatsConfig{
			Path:    "/admin/metrics.json",
			Enabled: true,
		},
		Health: HealthConfig{
			Path:    "/",
			Ok:      "OK",
			Enabled: true,
		},
	}
	if len(cfgFile) > 0 {
		file, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		cfg := NewDefaultConfig()
		json.Unmarshal(file, cfg)
	}
	return cfg

}

// Scheduler config, represented by json on disk.
//
type SchedConfig struct {
	initialized      bool                                  `json:"-"`
	ProtocolFactory  *thrift.TBinaryProtocolFactoryDefault `json:"-"`
	TransportFactory *thrift.TTransportFactory             `json:"-"`
	ClusterImpl      clusterdef.Cluster                    `json:"-"`
	CoordImpl        *saga.SagaCoordinator                 `json:"-"`
	DistImpl         *distributor.PoolDistributor          `json:"-"`
	WorkerFactory    worker.WorkerFactory                  `json:"-"`
	QueueImpl        queue.Queue                           `json:"-"`

	Cluster ClusterConfig `json:","`
	Queue   QueueConfig   `json:","`
	SagaLog SagaLogConfig `json:","`
	Worker  WorkerConfig  `json:","`
	Planner PlannerConfig `json:","`
	Stats   StatsConfig   `json:","`
	Health  HealthConfig  `json:","`
}

type ClusterConfig struct {
	Type    ClusterType `json:","`
	Initial int         `json:","`
	Count   *int        `json:",omitempty"` //memory
	Hosts   []string    `json:",omitempty"` //static
	Max     *int        `json:",omitempty"` //dynamic
}
type QueueConfig struct {
	Type     QueueType `json:","`
	Count    *int      `json:",omitempty"` //fake
	Capacity *int      `json:",omitempty"` //memory
}
type SagaLogConfig struct {
	Type SagaLogType `json:","`
}
type ExecConfig struct {
	Type ExecType `json:","`
}
type RunnerConfig struct {
	Type RunnerType `json:","`
	Exec ExecConfig `json:","`
}
type WorkerConfig struct {
	Type           WorkerType   `json:","`
	Runner         RunnerConfig `json:","`
	PingIntervalMs int          `json:","`
	TimeoutMs      int          `json:","`
}
type PlannerConfig struct {
	Type PlannerType `json:","`
}
type StatsConfig struct {
	Path    string `json:","`
	Enabled bool   `json:","`
}
type HealthConfig struct {
	Path    string `json:","`
	Ok      string `json:","`
	Enabled bool   `json:","`
}

//
//
type ClusterType string
type QueueType string
type SagaLogType string
type WorkerType string
type PlannerType string
type RunnerType string
type ExecType string

const (
	//
	ClusterMemory  ClusterType = "memory"
	ClusterStatic  ClusterType = "static"
	ClusterDynamic ClusterType = "dynamic"

	//
	QueueFake    QueueType = "fake"
	QueueMemory  QueueType = "memory"
	QueueDurable QueueType = "durable"

	//
	SagaLogMemory  SagaLogType = "memory"
	SagaLogDurable SagaLogType = "durable"

	//
	WorkerMemory WorkerType = "memory"
	WorkerThrift WorkerType = "thrift"

	//
	PlannerChaos  PlannerType = "chaos"
	PlannerSimple PlannerType = "simple"
	PlannerSmart  PlannerType = "smart"

	//
	RunnerSimple RunnerType = "simple"

	//
	ExecOs  ExecType = "os"
	ExecSim ExecType = "sim"
)

func IntToPtr(i int) *int { return &i }

/*
## Example json. Any subset can be specified to override specific defaults.
##
{
    "Cluster": {
        "Type": "memory",
        "Initial": 5,
        "Count": 10
    },
    "Queue": {
        "Type": "memory",
        "Capacity": 1000
    },
    "SagaLog": {
        "Type": "memory"
    },
    "Worker": {
        "Type": "memory",
        "Runner": {
            "Type": "simple",
            "Exec": {
                "Type": "os"
            }
        },
        "PingIntervalMs": 0,
        "TimeoutMs": 0
    },
    "Planner": {
        "Type": "simple"
    },
    "Stats": {
        "Path": "/admin/metrics.json",
        "Enabled": true
    },
    "Health": {
        "Path": "/",
        "Ok": "OK",
        "Enabled": true
    }
}

## For posterity, original pseudo json.
##
{
 "cluster": {"type": "memory",  "initial": 5, "count": 10} ||
            {"type": "static",  "initial": 5, "hosts": [":2345", ":2346"]} ||
            {"type": "dynamic", "initial": 5, "max": 6},

 "queue": {"type": "fake", "count": 1000} ||
          {"type": "memory", "capacity": 1000} ||
          {"type": "durable"}

 "sagalog": {"type": "memory"} ||
            {"type": "durable"}

 "worker": {"type": "memory", "runner": {"type": "simple", "exec": {"type": "os" } || {"type": "sim"}}} ||
           {"type": "thrift", "pingIntervalMs": "1000", "timeoutMs": 5000}

 "planner": {"type": "chaos"} ||
            {"type": "simple"} ||
            {"type": "smart"}

 "stats": {"path": "/admin/metrics.json", "enabled": true}
 "health": {"path": "/", "ok": "OK", "enabled": true}
}
*/

// func main() {
// 	bytes, err := json.MarshalIndent(DefaultSchedConfig, "", "    ")
// 	fmt.Println(string(bytes))
// 	fmt.Println(err)
// }
