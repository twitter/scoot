package sched

import (
	"fmt"
	"reflect"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/scootdev/scoot/common/thrifthelpers"
	"github.com/scootdev/scoot/sched/gen-go/schedthrift"
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
		t.Errorf("error: couldn't serialize a nil job. %s, %s\n", err.Error(), string(asByteArray))
	}
	if asByteArray, err := thrifthelpers.BinarySerialize(nil); err != nil || asByteArray != nil {
		t.Errorf("error: couldn't serialize a nil job. %s, %s\n", err.Error(), string(asByteArray))
	}

	job := schedthrift.Job{}
	var emptyBytes []byte
	if err := thrifthelpers.BinaryDeserialize(&job, emptyBytes); err != nil {
		t.Errorf("error: error deserializing and empyt byte array %s\n", err.Error())
	}

	if err := thrifthelpers.JsonDeserialize(&job, emptyBytes); err != nil {
		t.Errorf("error: error deserializing and empyt byte array %s\n", err.Error())
	}
}

func makeFixedSampleJob() *Job {
	job := Job{}
	jobId := "jobID"
	taskId := "task0"
	job.Id = jobId
	jobDef := JobDefinition{}
	jobDef.Tasks = make(map[string]TaskDefinition)
	jobDef.JobType = "jobTypeVal"
	taskDefinition := TaskDefinition{}
	taskDefinition.SnapshotID = "snapshotIDVal"
	taskDefinition.Timeout = 3
	taskDefinition.TaskID = taskId
	taskDefinition.JobID = jobId
	envVars := make(map[string]string)
	taskDefinition.EnvVars = envVars
	envVars["envVar1"] = "var2Value"
	envVars["envVar2"] = "var2Value"
	args := []string{"arg1", "arg2"}
	taskDefinition.Argv = args
	jobDef.Tasks[taskId] = taskDefinition
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
		log.Infof("error: couldn't serialize the fixed job def. %s", err.Error())
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
			log.Infof("serialize/deserialize test couldn't deserialize object:")
			log.Info(domainJob)
			log.Infof(fmt.Sprintf("Serialized to:%s\n", string(asByteArray)))
			log.Infof("error: deserializing the byte Array: %s\n%s\n", string(asByteArray), err.Error())
			return false

			// compare the orig and generated task definitions
		} else {
			newDomainJob = makeDomainJobFromThriftJob(newThriftJob)
			if !reflect.DeepEqual(domainJob, newDomainJob) || !reflect.DeepEqual(thriftJob, newThriftJob) {
				log.Infof("serialize/deserialize test didn't return equivalent value:")
				log.Infof("Original job: %+v", domainJob)
				log.Info("Deserialzied to: %+v", newDomainJob)
				return false
			}
		}
	}

	return true
}
