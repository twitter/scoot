package config

import (
	"time"

	"github.com/twitter/scoot/scheduler/server"
	"github.com/twitter/scoot/workerapi/client"
)

// SchedulerConfigs the map of available configurations
var ServiceConfigs = map[string]ServiceConfig{
	"default":      defaultConfig,
	"local.local":  localLocal,
	"local.memory": localMemory,
}

// defaultConfig the configuration values that are used for nil parts of a specific configuration and for integration tests
var defaultConfig = ServiceConfig{
	ClusterConfig{
		Type: "memory",
	},
	client.WorkersClientConfig{
		Type: "local",
	},
	server.SchedulerConfig{
		MaxRetriesPerTask:   0,
		DefaultTaskTimeout:  time.Duration(30) * time.Minute,
		RunnerRetryTimeout:  DefaultRunnerRetryTimeout,
		RunnerRetryInterval: DefaultRunnerRetryInterval,
		ReadyFnBackoff:      DefaultReadyFnBackoff,
	},
	SagaLogConfig{
		Type: "memory",
	},
}

// localLocal config for local.local - !!! make sure this constant is added to SchedulerConfigs map above !!!
var localLocal = ServiceConfig{
	ClusterConfig{
		Type: "local",
	},
	client.WorkersClientConfig{
		Type:          "rpc",
		PollingPeriod: time.Duration(250) * time.Millisecond,
	},
	server.SchedulerConfig{
		MaxRetriesPerTask:    0,
		DebugMode:            false,
		RecoverJobsOnStartup: true,
		DefaultTaskTimeout:   time.Duration(30) * time.Minute,
		RunnerRetryTimeout:   DefaultRunnerRetryTimeout,
		RunnerRetryInterval:  DefaultRunnerRetryInterval,
		ReadyFnBackoff:       DefaultReadyFnBackoff,
	},
	SagaLogConfig{
		Type:      "file",
		Directory: ".scootdata/filesagalog",
	},
}

// localMemory config for local.memory - !!! make sure this constant is added to SchedulerConfigs map above !!!
var localMemory = ServiceConfig{
	ClusterConfig{
		Type:  "memory",
		Count: 10,
	},
	client.WorkersClientConfig{
		Type: "local",
	},
	server.SchedulerConfig{
		MaxRetriesPerTask:    0,
		DebugMode:            false,
		RecoverJobsOnStartup: false,
		DefaultTaskTimeout:   time.Duration(30) * time.Minute,
		RunnerRetryTimeout:   DefaultRunnerRetryTimeout,
		RunnerRetryInterval:  DefaultRunnerRetryInterval,
		ReadyFnBackoff:       DefaultReadyFnBackoff,
	},
	SagaLogConfig{
		Type:          "memory",
		ExpirationSec: 0,
		GCIntervalSec: 1,
	},
}
