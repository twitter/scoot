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
		_, err := GetConfig(configSelector)
		assert.Nil(t, err, fmt.Sprintf("error getting schedule server config.  %s", err))
	}

	selector := "invalid.selector"
	config, err := GetConfig(selector)
	assert.NotNil(t, err, fmt.Sprintf("configuration returned for %s: %s", selector, config))
}

// TestCreatingConfigStruct test overriding default structure values with values from
// command line's specification.
func TestCreatingConfigStruct(t *testing.T) {
	config, err := GetConfig("local.local")
	assert.Nil(t, err)
	assert.Equal(t, "local", config.Cluster.Type)
	assert.Equal(t, "file", config.SagaLog.Type)
	assert.Equal(t, ".scootdata/filesagalog", config.SagaLog.Directory)

	assert.True(t, config.Scheduler.RecoverJobsOnStartup)
}
