package config

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/cloud/cluster/local"
	"github.com/twitter/scoot/scheduler/server"
	"github.com/twitter/scoot/workerapi/client"
)

// How long to keep retrying a runner req
const DefaultRunnerRetryTimeout = 10 * time.Second

// How long to sleep between runner req retries.
const DefaultRunnerRetryInterval = time.Second

// How long to wait between runner status queries to determine [init] status.
const DefaultReadyFnBackoff = 5 * time.Second

// SchedulerServerConfig config structure holding original json configs
type ServiceConfig struct {
	Cluster   ClusterConfig
	Workers   client.WorkersClientConfig
	Scheduler server.SchedulerConfig
	SagaLog   SagaLogConfig
}

func (sc ServiceConfig) String() string {
	return fmt.Sprintf("\n%s\n%s\n%s\n%s", sc.Cluster, sc.Workers, sc.Scheduler.String(), sc.SagaLog)
}

type ClusterConfig struct {
	Type  string // cluster type: local, memory
	Count int    // default to 10 for Type == memory
}

func (c ClusterConfig) String() string {
	return fmt.Sprintf("ClusterConfig: Type: %s, Count: %d", c.Type, c.Count)
}

type SagaLogConfig struct {
	Type          string // file
	Directory     string // default to .scootdata/filesagalog
	ExpirationSec int    // default to 0
	GCIntervalSec int    // default to 1
}

func (s SagaLogConfig) String() string {
	return fmt.Sprintf("SagaLogConfig: Type:%s, Directory: %s, ExpirationSec: %d, GCIntervalSec: %d",
		s.Type, s.Directory, s.ExpirationSec, s.GCIntervalSec)
}

func GetConfig(configSelector string) (*ServiceConfig, error) {
	config, ok := ServiceConfigs[configSelector]
	if !ok {
		keys := make([]string, 0, len(ServiceConfigs))
		for k := range ServiceConfigs {
			keys = append(keys, k)
		}
		return nil, fmt.Errorf("invalid configuration %s, supported values are %v", configSelector, keys)
	}

	return &config, nil
}

// GetSchedulerConfig get the scheduler config
func GetServiceConfig(configName string) (*ServiceConfig, error) {
	// get the default values, these will override any of the config
	// sections whose Type is ""
	defaultConfig, err := GetConfig("default")
	if err != nil {
		return nil, fmt.Errorf("couldn't parse the default config: %v", err)
	}

	// get the config values as per the command line config name
	schedServerConfig, err := GetConfig(configName)
	if err != nil {
		return nil, err
	}

	// use the default values for any sections whose type was not set in the command line config
	if schedServerConfig.Cluster.Type == "" {
		log.Infof("using default Cluster config")
		schedServerConfig.Cluster = defaultConfig.Cluster
	}
	if schedServerConfig.SagaLog.Type == "" {
		log.Infof("using default SagaLog config")
		schedServerConfig.SagaLog = defaultConfig.SagaLog
	}
	if schedServerConfig.Workers.Type == "" {
		log.Infof("using default Workers config")
		schedServerConfig.Workers = defaultConfig.Workers
	}

	return schedServerConfig, nil
}

// Parameters for configuring an in-memory Scoot cluster
// Count - number of in-memory workers
type ClusterMemoryConfig struct {
	Count int
}

func (c *ClusterMemoryConfig) Create() (*cluster.Cluster, error) {
	workerNodes := make([]cluster.Node, c.Count)
	for i := 0; i < c.Count; i++ {
		workerNodes[i] = cluster.NewIdNode(fmt.Sprintf("inmemory%d", i))
	}
	return cluster.NewCluster(workerNodes, nil), nil
}

// Parameters for configuring a Scoot cluster that will have locally-run components.
type ClusterLocalConfig struct{}

func (c *ClusterLocalConfig) Create() (*cluster.Cluster, error) {
	f := local.MakeFetcher("workerserver", "thrift_addr")
	updates := cluster.MakeFetchCron(f, time.NewTicker(time.Second).C)
	return cluster.NewCluster(nil, updates), nil
}
