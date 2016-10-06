// +build property

package sched

import (
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/scootdev/scoot/common/thrifthelpers"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched/gen-go/schedthrift"
	"github.com/scootdev/scoot/tests/testhelpers"
	"reflect"
	"testing"
	"time"
)

func Test_FixedJob(t *testing.T) {
	// use this test to test a specific jobDefinition struct
	schedJob := makeFixedSampleJob()

	ValidateSerialization(schedJob, false, t)
	ValidateSerialization(schedJob, true, t)
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

func ValidateSerialization(domainJob *Job, useJson bool, t *testing.T) {

	var asByteArray []byte
	var err error
	thriftJob, _ := makeThriftJobFromDomainJob(domainJob)
	if useJson {
		asByteArray, err = thrifthelpers.JsonSerialize(thriftJob)
	} else {
		asByteArray, err = thrifthelpers.BinarySerialize(thriftJob)
	}
	if err != nil {
		t.Errorf("error: couldn't serialize the fixed job def. %s\n", err.Error())

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
			t.Errorf("error: deserializing the byte Array: %s\n%s\n", string(asByteArray), err.Error())

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
				t.Errorf("fail: task definitions are not equal:\n")
			}
		}
	}
}

func GopterGenJob() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		jobDef := genJobFromParams(genParams)
		//Print(jobDef)
		genResult := gopter.NewGenResult(jobDef, gopter.NoShrinker)
		return genResult
	}
}

func genJobFromParams(genParams *gopter.GenParameters) *Job {

	//number of tasks to have in this saga
	numTasks := int(genParams.NextUint64() % 10)
	var taskDefMap = make(map[string]TaskDefinition)

	for i := 0; i < numTasks; i++ {
		snapshotId := fmt.Sprintf("snapShotId:%s", testhelpers.GenRandomAlphaNumericString(genParams.Rng))
		taskName := fmt.Sprintf("taskName:%s", testhelpers.GenRandomAlphaNumericString(genParams.Rng))

		numArgs := int(genParams.NextUint64() % 5)
		var j int
		var args []string = []string{}
		for j = 0; j < numArgs; j++ {
			args = append(args, fmt.Sprintf("arg%d:%s", j, testhelpers.GenRandomAlphaNumericString(genParams.Rng)))
		}

		var envVarsMap map[string]string = make(map[string]string)
		numEnvVars := int(genParams.NextUint64() % 5)
		for j = 0; j < numEnvVars; j++ {
			envVarsMap[fmt.Sprintf("env%d", j)] = testhelpers.GenRandomAlphaNumericString(genParams.Rng)
		}

		timeout := time.Duration(genParams.NextInt64() % 1000)

		cmd := runner.Command{SnapshotId: snapshotId, Argv: args, EnvVars: envVarsMap, Timeout: timeout}
		taskDef := TaskDefinition{cmd}
		taskDefMap[taskName] = taskDef

	}
	jobType := fmt.Sprintf("jobType:%s", testhelpers.GenRandomAlphaNumericString(genParams.Rng))
	jobDef := JobDefinition{JobType: jobType, Tasks: taskDefMap}

	job := Job{Id: testhelpers.GenRandomAlphaNumericString(genParams.Rng), Def: jobDef}

	return &job
}
