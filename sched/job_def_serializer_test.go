package sched

import (
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/scootdev/scoot/common/thrifthelpers"
	"github.com/scootdev/scoot/sched/gen-go/schedthrift"
	"github.com/scootdev/scoot/tests/testhelpers"
	"reflect"
	"testing"
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
	job.Id = "jobID"
	jobDef := JobDefinition{}
	jobDef.Tasks = make(map[string]TaskDefinition)
	jobDef.JobType = "jobTypeVal"
	taskDefinition := TaskDefinition{}
	taskDefinition.SnapshotId = "snapshotIdVal"
	taskDefinition.Timeout = 3
	envVars := make(map[string]string)
	taskDefinition.EnvVars = envVars
	envVars["envVar1"] = "var2Value"
	envVars["envVar2"] = "var2Value"
	args := []string{"arg1", "arg2"}
	taskDefinition.Argv = args
	jobDef.Tasks["task0"] = taskDefinition
	//Print(jobDef)  -I enable this for debugging
	job.Def = jobDef

	return &job
}

func Print(job *Job) {
	fmt.Printf(fmt.Sprintf("job id:%s\n", job.Id))
	fmt.Printf(fmt.Sprintf("job type:%s\n", job.Def.JobType))
	for taskName, taskDef := range job.Def.Tasks {
		fmt.Printf(fmt.Sprintf("taskName: %s\n", taskName))
		fmt.Printf(fmt.Sprintf("\ttimeout: %s\n", taskDef.Timeout.String()))
		fmt.Printf(fmt.Sprintf("\tsnapshotId: %s\n", taskDef.SnapshotId))
		for envVarName, envVarVal := range taskDef.EnvVars {
			fmt.Printf(fmt.Sprintf("\tenvVar:%s = %s\n", envVarName, envVarVal))
		}
		for i, arg := range taskDef.Argv {
			fmt.Printf(fmt.Sprintf("\targ[%d]:%s\n", i, arg))
		}
	}
	fmt.Printf("\n")
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
		fmt.Printf("error: couldn't serialize the fixed job def. %s\n", err.Error())
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
			fmt.Printf("serialize/deserialize test couldn't deserialize object:\n")
			Print(domainJob)
			fmt.Printf(fmt.Sprintf("Serialized to:%s\n", string(asByteArray)))
			fmt.Printf("error: deserializing the byte Array: %s\n%s\n", string(asByteArray), err.Error())
			return false

			// compare the orig and generated task definitions
		} else {
			newDomainJob = makeDomainJobFromThriftJob(newThriftJob)
			if !reflect.DeepEqual(domainJob, newDomainJob) || !reflect.DeepEqual(thriftJob, newThriftJob) {
				fmt.Printf("serialize/deserialize test didn't return equivalent value:\n")
				fmt.Printf("original jobDef:\n")
				Print(domainJob)
				fmt.Printf(fmt.Sprintf("Serialized to:%s\n", string(asByteArray)))
				fmt.Printf("deserialized to:\n")
				Print(newDomainJob)
				fmt.Printf("fail: task definitions are not equal:\n")
				return false
			}
		}
	}

	return true
}

func GopterGenJobDef() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		jobId := testhelpers.GenRandomAlphaNumericString(genParams.Rng)
		numTasks := genParams.Rng.Intn(10)
		job := GenRandomJob(jobId, numTasks, genParams.Rng)
		genResult := gopter.NewGenResult(job.Def, gopter.NoShrinker)
		return genResult
	}
}
