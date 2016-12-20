// +build property_test

package sched

import (
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/scootdev/scoot/tests/testhelpers"
	"testing"
)

func Test_RandomSerializerDeserializer(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 1000
	properties := gopter.NewProperties(parameters)

	properties.Property("Serialize JobDef", prop.ForAll(
		func(job *Job) bool {
			serializeBinary := ValidateSerialization(job, false)
			if !serializeBinary {
				return false
			}

			serializeJson := ValidateSerialization(job, true)
			if !serializeJson {
				return false
			}

			return true
		},

		GopterGenJob(),
	))

	properties.TestingRun(t)
}

func GopterGenJob() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		numTasks := genParams.Rng.Intn(10)
		jobId := testhelpers.GenRandomAlphaNumericString(genParams.Rng)
		job := GenRandomJob(jobId, numTasks, genParams.Rng)

		genResult := gopter.NewGenResult(&job, gopter.NoShrinker)
		return genResult
	}
}
