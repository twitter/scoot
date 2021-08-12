package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var tests = []string{"local.local"}

// Tests to ensure config is properly specified
func TestGettingConfigurations(t *testing.T) {
	for _, configSelector := range tests {
		_, err := GetWorkerConfigText(configSelector)
		assert.Nil(t, err, fmt.Sprintf("error getting schedule server config.  %s", err))
	}

	selector := "invalid.selector"
	config, err := GetWorkerConfigText(selector)
	assert.NotNil(t, err, fmt.Sprintf("configuration returned for %s: %s", selector, config))
}

// TestCreatingConfigStruct test overriding default structure values with values from
// command line's specification.
func TestCreatingConfigStruct(t *testing.T) {
	jsonConfig, err := GetWorkerConfigText("local.local")
	assert.Nil(t, err)

	assert.NotEmpty(t, jsonConfig)
}
