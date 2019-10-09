package main

import (
	"fmt"
	"github.com/twitter/scoot/scheduler/api"
	"github.com/twitter/scoot/scheduler/main/config"
	"testing"
)

var tests = []string{"local.memory", "local.local"}

// Tests to ensure config is properly specified
// and that they parse correctly
func TestConfigParses(t *testing.T) {
	_, schema := api.Defaults()

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
