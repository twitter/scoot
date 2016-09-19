package sched

import (
	"errors"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/scootdev/scoot/tests/testhelpers"
	"reflect"
	"strings"
	"testing"
	"time"
)

func Test_FixedJob(t *testing.T) {
	// use this test to test a specific jobDefinition struct
	origJob := makeFixedSampleJob()

	ValidateSerialization(origJob, BinarySerializer, t)
	ValidateSerialization(origJob, JsonSerializer, t)
}

func Test_SerializeNilJob(t *testing.T) {
	if asByteArray, err := SerializeJob(nil, JsonSerializer); err != nil || asByteArray != nil {
		t.Errorf("error: couldn't serialize a nil job. %s, %s\n", err.Error(), string(asByteArray))
	}
	if asByteArray, err := SerializeJob(nil, BinarySerializer); err != nil || asByteArray != nil {
		t.Errorf("error: couldn't serialize a nil job. %s, %s\n", err.Error(), string(asByteArray))
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

func Test_SerializationErrors(t *testing.T) {

	if _, err := SerializeJob(&Job{}, FakeSerializer); err == nil || !strings.Contains(err.Error(), "error writing\n") {
		t.Errorf("error: didn't get error serializing")
	}

	var bytes = []byte("unparsable byte string")
	var err error
	if _, err = DeserializeJob(bytes, JsonSerializer); err == nil {
		t.Errorf("error: didn't get error from json deserializing")
	}

	if _, err = DeserializeJob(bytes, BinarySerializer); err == nil {
		t.Errorf("error: didn't get error from binary deserializing")
	}
}

func Test_RandomSerializerDeserializer(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 1000
	properties := gopter.NewProperties(parameters)

	properties.Property("Serialize JobDef", prop.ForAll(
		func(job *Job) bool {
			ValidateSerialization(job, BinarySerializer, t)
			ValidateSerialization(job, JsonSerializer, t)
			return true
		},

		GopterGenJob(),
	))

	properties.TestingRun(t)

}

func ValidateSerialization(origJob *Job, serializer Serializer, t *testing.T) {

	if asByteArray, err := SerializeJob(origJob, serializer); err != nil {
		t.Errorf("error: couldn't serialize the fixed job def. %s\n", err.Error())

	} else {
		// deserialize the byte array
		if newJob, err := DeserializeJob(asByteArray, serializer); err != nil {
			fmt.Printf("serialize/deserialize test couldn't deserialize object:\n")
			Print(origJob)
			fmt.Printf(fmt.Sprintf("Serialized to:%s\n", string(asByteArray)))
			t.Errorf("error: deserializing the byte Array: %s\n%s\n", string(asByteArray), err.Error())

			// compare the orig and generated task definitions
		} else if !reflect.DeepEqual(origJob, newJob) {
			fmt.Printf("serialize/deserialize test didn't return equivalent value:\n")
			fmt.Printf("original jobDef:\n")
			Print(origJob)
			fmt.Printf(fmt.Sprintf("Serialized to:%s\n", string(asByteArray)))
			fmt.Printf("deserialized to:\n")
			Print(newJob)
			t.Errorf("fail: task definitions are not equal:\n")
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

	job := Job{}
	job.Id = testhelpers.GenRandomAlphaNumericString(genParams.Rng)

	jobDef := JobDefinition{}
	jobDef.JobType = fmt.Sprintf("jobType:%s", testhelpers.GenRandomAlphaNumericString(genParams.Rng))

	//number of tasks to run in this saga
	numTasks := int(genParams.NextUint64() % 10)
	var taskDefMap = make(map[string]TaskDefinition)

	for i := 0; i < numTasks; i++ {
		taskDef := TaskDefinition{}
		taskDef.SnapshotId = fmt.Sprintf("snapShotId:%s", testhelpers.GenRandomAlphaNumericString(genParams.Rng))
		taskName := fmt.Sprintf("taskName:%s", testhelpers.GenRandomAlphaNumericString(genParams.Rng))
		taskDef.SnapshotId = fmt.Sprintf("snapShotId:%s", testhelpers.GenRandomAlphaNumericString(genParams.Rng))

		numArgs := int(genParams.NextUint64() % 5)
		var j int
		var args []string = []string{}
		for j = 0; j < numArgs; j++ {
			args = append(args, fmt.Sprintf("arg%d:%s", j, testhelpers.GenRandomAlphaNumericString(genParams.Rng)))
		}
		taskDef.Argv = args

		var envVarsMap map[string]string = make(map[string]string)
		numEnvVars := int(genParams.NextUint64() % 5)
		for j = 0; j < numEnvVars; j++ {
			envVarsMap[fmt.Sprintf("env%d", j)] = testhelpers.GenRandomAlphaNumericString(genParams.Rng)
		}
		taskDef.EnvVars = envVarsMap

		timeoutVal := genParams.NextUint64() % 10000
		timeout, _ := time.ParseDuration(fmt.Sprintf("+%ds", timeoutVal))
		taskDef.Timeout = timeout

		taskDefMap[taskName] = taskDef

	}
	jobDef.Tasks = taskDefMap
	job.Def = jobDef

	return &job
}

type fakeSerializerForErrors struct{}

func (s fakeSerializerForErrors) Serialize(sourceStruct thrift.TStruct) (b []byte, err error) {
	return nil, errors.New("error writing\n")
}

func (s fakeSerializerForErrors) Deserialize(targetStruct thrift.TStruct, sourceBytes []byte) (err error) {
	return errors.New("this method should not be used\n")
}

var FakeSerializer Serializer = fakeSerializerForErrors{}
