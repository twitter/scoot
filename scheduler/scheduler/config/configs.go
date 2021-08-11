package config

// SchedulerConfigs the map of available configurations
var SchedulerConfigs = map[string]string{
	"default":      defaultConfig,
	"local.local":  localLocal,
	"local.memory": localMemory,
}

// defaultConfig the configuration values that are used for nil parts of a specific configuration
const defaultConfig = `
{
	"Cluster": {
	  "Type": "memory"
	},
	"Workers": {
	  "Type": "local"
	},
	"SchedulerConfig": {
	  "Type": "stateful",
	  "MaxRetriesPerTask" : 0,
	  "DefaultTaskTimeout" : "30m"
	},
	"SagaLog": {
	  "Type": "memory"
	}
}
`

// localLocal config for local.local - !!! make sure this constant is added to SchedulerConfigs map above !!!
const localLocal = `
{
	"Cluster": {
	  "Type": "local"
	},
	"Workers": {
	  "Type": "rpc",
	  "PollingPeriod": "250ms"
	},
	"SchedulerConfig": {
	  "Type": "stateful",
	  "MaxRetriesPerTask" : 0,
	  "DebugMode" : false,
	  "RecoverJobsOnStartup" : true,
	  "DefaultTaskTimeout" : "30m"
	},
	"SagaLog": {
	  "Type": "file",
	  "Directory": ".scootdata/filesagalog"
	}
  }
  `

// localMemory config for local.memory - !!! make sure this constant is added to SchedulerConfigs map above !!!
const localMemory = `
  {
	"Cluster": {
	  "Type": "memory",
	  "Count": 10
	},
	"SchedulerConfig": {
	  "Type": "stateful",
	  "MaxRetriesPerTask" : 0,
	  "DebugMode" : false,
	  "RecoverJobsOnStartup" : false,
	  "DefaultTaskTimeout" : "30m"
	},
	"SagaLog": {
	  "Type": "memory",
	  "ExpirationSec": 0,
	  "GCIntervalSec": 1
	}
  }
  `
