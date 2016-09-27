package main

import (
	"fmt"
	"github.com/scootdev/scoot/binaries/scheduler/config"
	apiserver "github.com/scootdev/scoot/scootapi/server"
	"testing"
)

var tests = []string{"inMemory.json", "local.json"}

// Tests to ensure config is properly specified for all desired env/dc combos
// and that they parse correctly
func TestConfigParses(t *testing.T) {
	_, schema := apiserver.Defaults()

	for _, configFile := range tests {
		config, err := config.Asset(fmt.Sprintf("config/%v", configFile))

		if err != nil {
			t.Errorf("Error Getting Config File: %v", configFile)
		}

		_, err = schema.Parse(config)
		if err != nil {
			t.Errorf("Error Parsing Config File %v, with Error %v", configFile, err)
		}
	}
}
