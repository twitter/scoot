package domain

import (
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"testing"
)

func Test_RandomSerializerDeserializer(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	properties.Property("Serialize JobDef", prop.ForAll(
		func(job *Job) bool {

			if serializeBinary := ValidateSerialization(job, false); !serializeBinary {
				return false
			}

			if serializeJson := ValidateSerialization(job, true); !serializeJson {
				return false
			}

			return true
		},

		GopterGenJob(),
	))

	properties.TestingRun(t)
}
