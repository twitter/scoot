package config

// WorkerConfigs the map of available configurations
var WorkerConfigsMap = map[string]WorkerConfigs{
	"default":     defaultConfig,
	"local.local": localLocal,
}

type WorkerConfigs struct {
}

var defaultConfig = WorkerConfigs{}

// localLocal config for local.local - !!! make sure this constant is added to WorkerConfigs map above !!!
var localLocal = WorkerConfigs{}
