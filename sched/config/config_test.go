package config

import (
	"encoding/json"
	"testing"
)

func TestConfigRoundtrip(t *testing.T) {
	jsonBegin := `{
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
}`
	var cluster ClusterConfig
	var queue QueueConfig
	var sagaCoord SagaCoordConfig
	var worker WorkerConfig
	var planner PlannerConfig
	var stats StatsConfig
	var health HealthConfig
	realizedCfg := Config{
		Cluster:   &cluster,
		Queue:     &queue,
		SagaCoord: &sagaCoord,
		Worker:    &worker,
		Planner:   &planner,
		Stats:     &stats,
		Health:    &health,
	}
	var rawCfg rawConfig

	if err := json.Unmarshal([]byte(jsonBegin), &rawCfg); err != nil {
		t.Fatalf("Error reading raw config: %v.", err)
	}
	if err := json.Unmarshal(rawCfg.Cluster, realizedCfg.Cluster); err != nil {
		t.Fatalf("Error reading cluster config: %v.", err)
	}
	if err := json.Unmarshal(rawCfg.Queue, &realizedCfg.Queue); err != nil {
		t.Fatalf("Error reading queue config: %v.", err)
	}
	if err := json.Unmarshal(rawCfg.SagaCoord, &realizedCfg.SagaCoord); err != nil {
		t.Fatalf("Error reading sagaCoord config: %v.", err)
	}
	if err := json.Unmarshal(rawCfg.Worker, &realizedCfg.Worker); err != nil {
		t.Fatalf("Error reading worker config: %v.", err)
	}
	if err := json.Unmarshal(rawCfg.Planner, &realizedCfg.Planner); err != nil {
		t.Fatalf("Error reading planner config: %v.", err)
	}
	if err := json.Unmarshal(rawCfg.Stats, &realizedCfg.Stats); err != nil {
		t.Fatalf("Error reading stats config: %v.", err)
	}
	if err := json.Unmarshal(rawCfg.Health, &realizedCfg.Health); err != nil {
		t.Fatalf("Error reading health config: %v.", err)
	}

	bytes, err := json.MarshalIndent(&realizedCfg, "", "    ")
	if err != nil {
		t.Fatalf("Error encoding Config to json: %v.", err)
	}

	jsonEnd := string(bytes)
	if jsonEnd != jsonBegin {
		t.Fatalf("Error converting back to json, before/after:\n^%v$\n#####\n^%v$", jsonBegin, jsonEnd)
	}
}
