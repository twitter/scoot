// +build property_test

package sched

import (
  "github.com/leanovate/gopter"
  "github.com/leanovate/gopter/prop"
  "testing"
  "reflect"
)

func Test_JobSerializeDeserialize(t *testing.T) {
  parameters := gopter.DefaultTestParameters()
  parameters.MinSuccessfulTests = 1000
  properties := gopter.NewProperties(parameters)

  properties.Property("Serialize and Deserialize Job", prop.ForAll(
    func(job *Job) bool {

      binaryJob := job.Serialize()
      deserializedJob := DeserializeJob(binaryJob)

      return reflect.DeepEqual(job, deserializedJob)
    },
    GopterGenJob(),
  )),
  properties.TestingRun(t)
}