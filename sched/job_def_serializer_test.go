package sched

import (
	"reflect"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/thrifthelpers"
	"github.com/twitter/scoot/sched/gen-go/schedthrift"
)

func Test_FixedJob(t *testing.T) {
	// use this test to test a specific jobDefinition struct
	schedJob := makeFixedSampleJob()

	if !ValidateSerialization(schedJob, false) {
		t.Errorf("Could not validate binary serialization")
	}
	if !ValidateSerialization(schedJob, true) {
		t.Errorf("Could not validate json serialization")
	}
}

func Test_SerializeNilJob(t *testing.T) {
	if asByteArray, err := thrifthelpers.JsonSerialize(nil); err != nil || asByteArray != nil {
		t.Errorf("Error: couldn't serialize a nil job. %s, %s\n", err.Error(), string(asByteArray))
	}
	if asByteArray, err := thrifthelpers.BinarySerialize(nil); err != nil || asByteArray != nil {
		t.Errorf("Error: couldn't serialize a nil job. %s, %s\n", err.Error(), string(asByteArray))
	}

	job := schedthrift.Job{}
	var emptyBytes []byte
	if err := thrifthelpers.BinaryDeserialize(&job, emptyBytes); err != nil {
		t.Errorf("Error deserializing and empyt byte array %s\n", err.Error())
	}

	if err := thrifthelpers.JsonDeserialize(&job, emptyBytes); err != nil {
		t.Errorf("Error deserializing and empyt byte array %s\n", err.Error())
	}
}

func makeFixedSampleJob() *Job {
	job := Job{}
	jobId := "jobID"
	job.Id = jobId
	jobDef := JobDefinition{}
	jobDef.Tasks = []TaskDefinition{}
	jobDef.JobType = "jobTypeVal"
	jobDef.Requestor = "requestor"
	jobDef.Tag = "tag"
	taskDefinition := TaskDefinition{}
	taskDefinition.SnapshotID = "snapshotIDVal"
	taskDefinition.Timeout = 3
	taskDefinition.TaskID = "taskId0"
	taskDefinition.JobID = jobId
	taskDefinition.Tag = "tag"
	envVars := make(map[string]string)
	taskDefinition.EnvVars = envVars
	envVars["envVar1"] = "var2Value"
	envVars["envVar2"] = "var2Value"
	taskDefinition.Argv = []string{"arg1", "arg2"}
	jobDef.Tasks = append(jobDef.Tasks, taskDefinition)
	taskDefinition.TaskID = "taskId1"
	taskDefinition.Argv = []string{"argA", "argB"}
	jobDef.Tasks = append(jobDef.Tasks, taskDefinition)
	job.Def = jobDef

	return &job
}

func ValidateSerialization(domainJob *Job, useJson bool) bool {
	var asByteArray []byte
	var err error
	thriftJob, _ := makeThriftJobFromDomainJob(domainJob)
	if useJson {
		asByteArray, err = thrifthelpers.JsonSerialize(thriftJob)
	} else {
		asByteArray, err = thrifthelpers.BinarySerialize(thriftJob)
	}
	if err != nil {
		log.Infof("Error: couldn't serialize the fixed job def. %s", err.Error())
		return false

	} else {
		// deserialize the byte array
		var newThriftJob = schedthrift.NewJob()
		var newDomainJob *Job
		if useJson {
			err = thrifthelpers.JsonDeserialize(newThriftJob, asByteArray)
		} else {
			err = thrifthelpers.BinaryDeserialize(newThriftJob, asByteArray)
		}
		if err != nil {
			log.Infof("Serialize/deserialize test couldn't deserialize object: %+v", domainJob)
			log.Infof("Error deserializing the byte Array: %s\n%s\n", string(asByteArray), err.Error())
			return false

			// compare the orig and generated task definitions
		} else {
			newDomainJob = makeDomainJobFromThriftJob(newThriftJob)
			if !reflect.DeepEqual(domainJob, newDomainJob) || !reflect.DeepEqual(thriftJob, newThriftJob) {
				log.Info("Serialize/deserialize test didn't return equivalent value:")
				log.Infof("Original job:\n\n%+v\n", domainJob)
				log.Infof("Deserialized to:\n\n%+v\n", newDomainJob)
				return false
			}
		}
	}

	return true
}
