package config

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/cloud/cluster/local"
	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/scheduler/server"
	"github.com/twitter/scoot/worker/client"
)

// How long to keep retrying a runner req
const DefaultRunnerRetryTimeout = 10 * time.Second

// How long to sleep between runner req retries.
const DefaultRunnerRetryInterval = time.Second

// How long to wait between runner status queries to determine [init] status.
const DefaultReadyFnBackoff = 5 * time.Second

// SchedulerServerConfig config structure holding original json configs
type JSONConfigs struct {
	Cluster   ClusterJSONConfig              `json:"Cluster"`
	Workers   client.WorkersClientJSONConfig `json:"Workers"`
	Scheduler SchedulerJSONConfig            `json:"SchedulerConfig"`
	SagaLog   SagaLogJSONConfig              `json:"SagaLog"`
}

func (s JSONConfigs) String() string {
	return fmt.Sprintf("\n%s\n%s\n%s\n%s", s.Cluster, s.Workers, s.Scheduler, s.SagaLog)
}

type ClusterJSONConfig struct {
	Type  string `json:"Type"`  // cluster type: local, memory
	Count int    `json:"Count"` // default to 10 for Type == memory
}

func (c ClusterJSONConfig) String() string {
	return fmt.Sprintf("ClusterJSONConfig: Type: %s, Count: %d", c.Type, c.Count)
}

type SagaLogJSONConfig struct {
	Type          string `json:"Type"`          // file
	Directory     string `json:"Directory"`     // default to .scootdata/filesagalog
	ExpirationSec int    `json:"ExpirationSec"` // default to 0
	GCIntervalSec int    `json:"GCIntervalSec"` // default to 1
}

func (s SagaLogJSONConfig) String() string {
	return fmt.Sprintf("SagaLogJSONConfig: Type:%s, Directory: %s, ExpirationSec: %d, GCIntervalSec: %d",
		s.Type, s.Directory, s.ExpirationSec, s.GCIntervalSec)
}

type SchedulerJSONConfig struct {
	Type                 string `json:"Type"`              // scheduler type: stateful
	MaxRetriesPerTask    int    `json:"MaxRetriesPerTask"` // default to 0
	MaxRequestors        int    `json:"MaxRequestors"`
	MaxJobsPerRequestor  int    `json:"MaxJobsPerRequestor"`
	DebugMode            bool   `json:"DebugMode"`            // default to false
	RecoverJobsOnStartup bool   `json:"RecoverJobsOnStartup"` // default to true
	DefaultTaskTimeout   string `json:"DefaultTaskTimeout"`   // default to 30m
}

func (sc SchedulerJSONConfig) String() string {
	return fmt.Sprintf("SchedulerJSONConfig: Type: %s, MaxRetriesPerTask: %d, MaxRequestors: %d, MaxJobsPerRequestor: %d, DebugMode: %t, "+
		"RecoverJobsOnStartup: %t, DefaultTaskTimeout: %s",
		sc.Type, sc.MaxRetriesPerTask, sc.MaxRequestors, sc.MaxJobsPerRequestor, sc.DebugMode, sc.RecoverJobsOnStartup, sc.DefaultTaskTimeout)
}

func GetConfigText(configSelector string) ([]byte, error) {
	configText, ok := SchedulerConfigs[configSelector]
	if !ok {
		keys := make([]string, 0, len(SchedulerConfigs))
		for k := range SchedulerConfigs {
			keys = append(keys, k)
		}
		return nil, fmt.Errorf("invalid configuration %s, supported values are %v", configSelector, keys)
	}

	return []byte(configText), nil
}

func (jc *SchedulerJSONConfig) CreateSchedulerConfig() (*server.SchedulerConfiguration, error) {
	var err error
	serverConfig := &server.SchedulerConfiguration{}
	if jc.DefaultTaskTimeout != "" {
		serverConfig.DefaultTaskTimeout, err = time.ParseDuration(jc.DefaultTaskTimeout)
		if err != nil {
			return nil, err
		}
	}

	serverConfig.MaxRetriesPerTask = jc.MaxRetriesPerTask
	serverConfig.DebugMode = jc.DebugMode
	serverConfig.RecoverJobsOnStartup = jc.RecoverJobsOnStartup
	serverConfig.RunnerRetryTimeout = DefaultRunnerRetryTimeout
	serverConfig.RunnerRetryInterval = DefaultRunnerRetryInterval
	serverConfig.ReadyFnBackoff = DefaultReadyFnBackoff
	serverConfig.MaxRequestors = jc.MaxRequestors
	serverConfig.MaxJobsPerRequestor = jc.MaxJobsPerRequestor
	return serverConfig, nil
}

// GetSchedulerConfig get the scheduler config
func GetSchedulerConfigs(configName string) (*JSONConfigs, error) {
	// get the default values, these will override any of the config
	// sections whose Type is ""
	defaultConfigText, _ := GetConfigText("default")
	defaultConfig := &JSONConfigs{}
	err := json.Unmarshal(defaultConfigText, &defaultConfig)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse the default config: %v", err)
	}

	// get the config values as per the command line config name
	configText, err := GetConfigText(configName)
	if err != nil {
		return nil, err
	}

	schedServerConfig := &JSONConfigs{}
	err = json.Unmarshal(configText, &schedServerConfig)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse top-level config: %v", err)
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
	if schedServerConfig.Scheduler.Type == "" {
		log.Infof("using default Scheduler config")
		schedServerConfig.Scheduler = defaultConfig.Scheduler
	}

	return schedServerConfig, nil
}

// Parameters for configuring an in-memory Scoot cluster
// Count - number of in-memory workers
type ClusterMemoryConfig struct {
	Count int
}

func (c *ClusterMemoryConfig) Create() (chan []cluster.NodeUpdate, error) {
	workerNodes := make([]cluster.Node, c.Count)
	for i := 0; i < c.Count; i++ {
		workerNodes[i] = cluster.NewIdNode(fmt.Sprintf("inmemory%d", i))
	}
	fetcher := &MemoryFetcher{nodes: workerNodes}
	nuc, _ := cluster.NewCluster(nil, fetcher, true, 10*time.Minute, common.DefaultClusterChanSize)
	return nuc, nil
}

// Parameters for configuring a Scoot cluster that will have locally-run components.
type ClusterLocalConfig struct{}

func (c *ClusterLocalConfig) Create() (chan []cluster.NodeUpdate, error) {
	f := local.MakeFetcher("workerserver", "thrift_addr")
	nuc, _ := cluster.NewCluster(nil, f, true, 100*time.Millisecond, common.DefaultClusterChanSize)
	return nuc, nil
}

// MemoryFetcher is a fetcher that always returns a fixed set of nodes
type MemoryFetcher struct {
	nodes []cluster.Node
}

func (f *MemoryFetcher) Fetch() ([]cluster.Node, error) {
	return f.nodes, nil
}
