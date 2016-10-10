// +build property_test

package sched

import (
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"testing"
)

func Test_RandomSerializerDeserializer(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 1000
	properties := gopter.NewProperties(parameters)

	properties.Property("Serialize JobDef", prop.ForAll(
		func(job *Job) bool {
			ValidateSerialization(job, false, t)
			ValidateSerialization(job, true, t)
			return true
		},

		GopterGenJob(),
	))

	properties.TestingRun(t)

}
