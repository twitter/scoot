package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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

	"golang.org/x/net/context"
)

//
//TODO: handle and propagate all config values. handle errors. decide extensibility story.
//

// Return a Config where each field is initialized only if there is no plugin for it.
func DefaultConfig(cfgFile string, plugins Plugins) (*Config, error) {
	rawCfg, _ := defaultConfig(cfgFile)
	realizedCfg := Config{raw: rawCfg, plugins: plugins}

	//TODO: catch errors unmarshaling.
	if _, ok := plugins[ClusterTypeId]; !ok {
		var cfg ClusterConfig
		realizedCfg.Cluster = &cfg
		json.Unmarshal(rawCfg.Cluster, realizedCfg.Cluster)
	}
	if _, ok := plugins[QueueTypeId]; !ok {
		var cfg QueueConfig
		realizedCfg.Queue = &cfg
		json.Unmarshal(rawCfg.Queue, &realizedCfg.Queue)
	}
	if _, ok := plugins[SagaCoordTypeId]; !ok {
		var cfg SagaCoordConfig
		realizedCfg.SagaCoord = &cfg
		json.Unmarshal(rawCfg.SagaCoord, &realizedCfg.SagaCoord)
	}
	if _, ok := plugins[WorkerTypeId]; !ok {
		var cfg WorkerConfig
		realizedCfg.Worker = &cfg
		json.Unmarshal(rawCfg.Worker, &realizedCfg.Worker)
	}
	if _, ok := plugins[PlannerTypeId]; !ok {
		var cfg PlannerConfig
		realizedCfg.Planner = &cfg
		json.Unmarshal(rawCfg.Planner, &realizedCfg.Planner)
	}
	if _, ok := plugins[StatsTypeId]; !ok {
		var cfg StatsConfig
		realizedCfg.Stats = &cfg
		json.Unmarshal(rawCfg.Stats, &realizedCfg.Stats)
	}
	if _, ok := plugins[HealthTypeId]; !ok {
		var cfg HealthConfig
		realizedCfg.Health = &cfg
		json.Unmarshal(rawCfg.Health, &realizedCfg.Health)
	}

	return &realizedCfg, nil
}

// Main entry point, gets everything ready for 'main' to serve.
//TODO: check errors, add planner type, do health and stats.
func ConfigureCloudScoot(
	cfg *Config, ctx context.Context,
	transportFactory thrift.TTransportFactory,
	protocolFactory *thrift.TBinaryProtocolFactory,
) (scoot.CloudScoot, error) {
	var clusterImpl clusterdef.Cluster
	var sagaCoordImpl *saga.SagaCoordinator
	var workerFactory worker.WorkerFactory
	var queueImpl queue.Queue

	// Construct types from plugin if specified, else construct using Config.
	if plugin, ok := cfg.plugins[ClusterTypeId]; ok {
		impl, _ := plugin(cfg.raw.Cluster)
		clusterImpl = impl.(clusterdef.Cluster)
	} else {
		clusterImpl, _ = CreateCluster(cfg.Cluster)
	}

	if plugin, ok := cfg.plugins[SagaCoordTypeId]; ok {
		impl, _ := plugin(cfg.raw.SagaCoord)
		sagaCoordImpl = impl.(*saga.SagaCoordinator)
	} else {
		sagaCoordImpl, _ = CreateSagaCoord(cfg.SagaCoord)
	}

	if plugin, ok := cfg.plugins[WorkerTypeId]; ok {
		factory, _ := plugin(cfg.raw.Worker)
		workerFactory = factory.(worker.WorkerFactory)
	} else {
		workerFactory, _ = CreateWorkerFactory(cfg.Worker, transportFactory, protocolFactory)
	}

	if plugin, ok := cfg.plugins[QueueTypeId]; ok {
		impl, _ := plugin(cfg.raw.Queue)
		queueImpl = impl.(queue.Queue)
	} else {
		queueImpl, _ = CreateQueue(cfg.Queue)
	}

	// Make the scheduler and handler
	distImpl, _ := CreateDistributor(clusterImpl)
	schedImpl := scheduler.NewScheduler(distImpl, *sagaCoordImpl, workerFactory)
	handler := server.NewHandler(queueImpl, *sagaCoordImpl)

	// Go Routine which takes data from work queue and schedules it
	go func() {
		log.Println("Starting Scheduler")
		scheduler.GenerateWork(schedImpl, queueImpl.Chan())
	}()

	return handler, nil
}

//
func CreateCluster(cfg *ClusterConfig) (clusterdef.Cluster, error) {
	// TODO: replace with actual cluster implementation, currently dummy in memory cluster
	// Create the cluster nodes.
	workerNodes := []clusterdef.Node{}
	switch cfg.Type {
	case ClusterMemory:
		for idx := 0; idx < *cfg.Count; idx++ {
			workerNodes = append(workerNodes, clusterimpl.NewIdNode(fmt.Sprintf("inmemory%d", idx)))
		}
	case ClusterStatic:
		for idx := 0; idx < len(cfg.Hosts); idx++ {
			workerNodes = append(workerNodes, clusterimpl.NewIdNode(cfg.Hosts[idx]))
		}
	case ClusterDynamic:
		//TODO
	}
	cluster := clusterimpl.NewCluster(workerNodes, nil)
	return cluster, nil
}

//
func CreateSagaCoord(cfg *SagaCoordConfig) (*saga.SagaCoordinator, error) {
	// TODO: Replace with Durable SagaLog, currently In Memory Only
	sagaCoord := saga.MakeInMemorySagaCoordinator()
	return &sagaCoord, nil
}

//
func CreateWorkerFactory(
	cfg *WorkerConfig,
	transportFactory thrift.TTransportFactory,
	protocolFactory *thrift.TBinaryProtocolFactory,
) (worker.WorkerFactory, error) {
	workerFactory := fake.MakeWaitingNoopWorker
	if cfg.Type != WorkerMemory {
		workerFactory = func(node clusterdef.Node) worker.Worker {
			return rpc.NewThriftWorker(transportFactory, protocolFactory, string(node.Id()))
		}
	}
	return workerFactory, nil
}

//
func CreateQueue(cfg *QueueConfig) (queue.Queue, error) {
	// TODO: Replace with Durable WorkQueue, currently in Memory Only
	workQueue := queueimpl.NewSimpleQueue(*cfg.Capacity)
	return workQueue, nil
}

//
func CreateDistributor(cluster clusterdef.Cluster) (*distributor.PoolDistributor, error) {
	dist, err := distributor.NewPoolDistributorFromCluster(cluster)
	if err != nil {
		log.Fatalf("Error subscribing to cluster: %v", err)
	}
	return dist, nil
}

//
//TODO: expect fully specified JSON cfg and skip this back and forth marshaling nonsense.
func defaultConfig(cfgFile string) (*rawConfig, error) {
	realizedCfg := &Config{
		Cluster: &ClusterConfig{
			Type:    ClusterMemory,
			Initial: 5,
			Count:   IntToPtr(10),
		},
		Queue: &QueueConfig{
			Type:     QueueMemory,
			Capacity: IntToPtr(1000),
		},
		SagaCoord: &SagaCoordConfig{
			Type: SagaCoordMemory,
		},
		Worker: &WorkerConfig{
			Type: WorkerMemory,
			Runner: RunnerConfig{
				Type: RunnerSimple,
				Exec: ExecConfig{
					Type: ExecOs,
				},
			},
		},
		Planner: &PlannerConfig{
			Type: PlannerSimple,
		},
		Stats: &StatsConfig{
			Path:    "/admin/metrics.json",
			Enabled: true,
		},
		Health: &HealthConfig{
			Path:    "/",
			Ok:      "OK",
			Enabled: true,
		},
	}

	var bytes []byte
	var err error
	if len(cfgFile) == 0 {
		bytes, err = json.Marshal(&realizedCfg)
		if err != nil {
			return nil, err
		}
	} else {
		bytes, err = ioutil.ReadFile(cfgFile)
		if err != nil {
			return nil, err
		}
	}
	var rawCfg rawConfig
	err = json.Unmarshal(bytes, &rawCfg)

	return &rawCfg, err

}

// Scheduler config, represented by json on disk.
//
type rawConfig struct {
	Cluster   json.RawMessage `json:","`
	Queue     json.RawMessage `json:","`
	SagaCoord json.RawMessage `json:","`
	Worker    json.RawMessage `json:","`
	Planner   json.RawMessage `json:","`
	Stats     json.RawMessage `json:","`
	Health    json.RawMessage `json:","`
}

// Public config that allows one-off changes after parsing the config file.
type Config struct {
	raw     *rawConfig
	plugins Plugins

	Cluster   *ClusterConfig
	Queue     *QueueConfig
	SagaCoord *SagaCoordConfig
	Worker    *WorkerConfig
	Planner   *PlannerConfig
	Stats     *StatsConfig
	Health    *HealthConfig
}

// Configs for each type we want to construct.
//
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
type SagaCoordConfig struct {
	Type SagaCoordType `json:","`
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

// Takes a subsection of the config file and constructs the appropriate type.
type Plugins map[TypeId]PluginFn
type PluginFn func(json.RawMessage) (interface{}, error)
type PluginType string

// All the types we construct based on the config.
type TypeId string
type ClusterType PluginType
type SagaCoordType PluginType
type WorkerType PluginType
type QueueType PluginType
type PlannerType PluginType
type RunnerType string //PluginType
type ExecType string   //PluginType

const (
	//
	ClusterTypeId   TypeId = "cluster"
	SagaCoordTypeId TypeId = "sagaCoord"
	WorkerTypeId    TypeId = "worker"
	QueueTypeId     TypeId = "queue"
	PlannerTypeId   TypeId = "planner"
	RunnerTypeId    TypeId = "runner"
	ExecTypeId      TypeId = "exec"
	StatsTypeId     TypeId = "stats"
	HealthTypeId    TypeId = "health"

	//
	ClusterMemory  ClusterType = "memory"
	ClusterStatic  ClusterType = "static"
	ClusterDynamic ClusterType = "dynamic"

	//
	QueueFake    QueueType = "fake"
	QueueMemory  QueueType = "memory"
	QueueDurable QueueType = "durable"

	//
	SagaCoordMemory  SagaCoordType = "memory"
	SagaCoordDurable SagaCoordType = "durable"

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
    "SagaCoord": {
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
