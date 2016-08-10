package config_test

import (
	"encoding/json"
	"testing"

	"github.com/scootdev/scoot/sched/config"
)

func TestConfigRoundtrip(t *testing.T) {
	before := `{
 "Queue": {
  "Type": "memory",
  "Capacity": 1000
 },
 "Cluster": {
  "Type": "memory",
  "Count": 10
 },
 "SagaLog": {
  "Type": ""
 },
 "Workers": {
  "Type": "local"
 }
}`

	p := config.Default()
	cfg, err := p.Parse([]byte(before))
	if err != nil {
		t.Fatalf("Error parsing before: %v", err)
	}

	bytes, err := json.MarshalIndent(&cfg, "", " ")
	if err != nil {
		t.Fatalf("Error encoding Config to json: %v.", err)
	}
	after := string(bytes)
	if before != after {
		t.Fatalf("Error converting back to json, before/after:\n^%v$\n#####\n^%v$", before, after)
	}
}
