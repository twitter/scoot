package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var tests = []string{"local.memory", "local.local"}

// Tests to ensure config is properly specified
// and that they parse correctly
func TestGettingConfigurations(t *testing.T) {
	for _, configSelector := range tests {
		_, err := GetSchedulerConfigs(configSelector)
		assert.Nil(t, err, fmt.Sprintf("error getting schedule server config.  %s", err))
	}

	selector := "invalid.selector"
	config, err := GetSchedulerConfigs(selector)
	assert.NotNil(t, err, fmt.Sprintf("configuration returned for %s: %s", selector, config))
}

// TestCreatingConfigStruct test overriding default structure values with values from
// command line's specification.
func TestCreatingConfigStruct(t *testing.T) {
	jsonConfig, err := GetSchedulerConfigs("local.local")
	assert.Nil(t, err)
	assert.Equal(t, "local", jsonConfig.Cluster.Type)
	assert.Equal(t, "file", jsonConfig.SagaLog.Type)
	assert.Equal(t, ".scootdata/filesagalog", jsonConfig.SagaLog.Directory)

	config, err := jsonConfig.Scheduler.CreateSchedulerConfig()
	assert.Nil(t, err)
	assert.True(t, config.RecoverJobsOnStartup)
}
