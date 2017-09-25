// +build property_test

package sched

import (
	"reflect"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	log "github.com/sirupsen/logrus"
)

func Test_JobSerializeDeserialize(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 1000
	properties := gopter.NewProperties(parameters)

	properties.Property("Serialize and Deserialize Job", prop.ForAll(
		func(job *Job) bool {

			binaryJob, err := job.Serialize()
			if err != nil {
				log.Info("Unexpected Error Occurred when Serializing Job %v", err)
				return false
			}

			deserializedJob, err := DeserializeJob(binaryJob)
			if err != nil {
				log.Info("Unexpected Error Occurred when Deserializing Job %v", err)
				return false
			}
			return reflect.DeepEqual(job, deserializedJob)
		},
		GopterGenJob(),
	))

	properties.TestingRun(t)
}
