package config

import (
	"encoding/json"
	"fmt"
)

func GetWorkerConfigText(configSelector string) ([]byte, error) {
	configText, ok := WorkerConfigsMap[configSelector]
	if !ok {
		keys := make([]string, 0, len(WorkerConfigsMap))
		for k := range WorkerConfigsMap {
			keys = append(keys, k)
		}
		return nil, fmt.Errorf("invalid worker configuration %s, supported values are %v", configSelector, keys)
	}

	configBytes, err := json.Marshal(configText)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse the worker default config: %v", err)
	}

	return configBytes, nil
}
