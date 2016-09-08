package sched

import (
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/scootdev/scoot/tests/testhelpers"
	"strings"
	"testing"
	"time"
)

const (
	testTaskName = "task0"
	envVar1Name  = "envVar1"
	envVar2Name  = "envVar2"
)

func Test_JobCompare(t *testing.T) {
	origJob := makeFixedSampleJob()

	// compare object to itself
	if ok, msg := origJob.Equal(origJob); !ok {
		t.Errorf("error: comparison logic doesn't work comparing an object with itself. %s\n", msg)
	}

	// compare object to nil
	var actualJob *Job
	if ok, msg := origJob.Equal(actualJob); ok {
		t.Errorf("error: comparison logic doesn't work comparing an object vs nil. %s\n", msg)
	}

	actualJob = makeFixedSampleJob()

	// compare nil to object
	var saveOrigJob *Job = origJob
	origJob = nil
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "caller was nil, but other job was not") {
		t.Errorf("error: comparison logic doesn't work comparing an nil vs object. %s\n", msg)
	}
	origJob = saveOrigJob

	//  compare object to equivalent object
	if ok, msg := origJob.Equal(actualJob); !ok {
		t.Errorf("error: comparison logic doesn't work comparing an object against equivalent. %s\n", msg)
	}

	var savedStringVal string
	// compare when job Ids don't match
	actualJob.Id, savedStringVal = "ids not equal", actualJob.Id
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "job Ids differ") {
		t.Errorf("error: comparison logic doesn't work comparing an objects with different job Ids. %s\n", msg)
	}
	actualJob.Id = savedStringVal

	// compare when job Tasks don't match
	// job types differ
	actualJob.Def.JobType, savedStringVal = "job type not equal", actualJob.Def.JobType
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "job Types differ") {
		t.Errorf("error: comparison logic doesn't work comparing object with different job types. %s\n", msg)
	}
	actualJob.Def.JobType = savedStringVal

	// different task snapshotid
	savedTaskDef := actualJob.Def.Tasks[testTaskName]
	var modifiedTaskDef TaskDefinition = actualJob.Def.Tasks[testTaskName]
	modifiedTaskDef.SnapshotId = "snapshot id not equal"
	actualJob.Def.Tasks[testTaskName] = modifiedTaskDef
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "Snapshot ids differ") {
		t.Errorf("error: comparison logic doesn't work comparing an objects with different task definition snapshot ids: message: %s\n", msg)
	}
	actualJob.Def.Tasks[testTaskName] = savedTaskDef

	// different timeout
	modifiedTaskDef = actualJob.Def.Tasks[testTaskName]
	modifiedTaskDef.Timeout, _ = time.ParseDuration("+9s")
	actualJob.Def.Tasks[testTaskName] = modifiedTaskDef
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "Timeout values differ") {
		t.Errorf("error: comparison logic doesn't work comparing an objects with different task definition timeout values: message: %s\n", msg)
	}
	actualJob.Def.Tasks[testTaskName] = savedTaskDef

	// different args
	modifiedTaskDef = actualJob.Def.Tasks[testTaskName]
	modifiedTaskDef.Argv = []string{"args don't match"}
	actualJob.Def.Tasks[testTaskName] = modifiedTaskDef
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "Argv entries are different") {
		t.Errorf("error: comparison logic doesn't work comparing an objects with different task definition args: message: %s\n", msg)
	}
	actualJob.Def.Tasks[testTaskName] = savedTaskDef

	// empty args vs non-nil args
	modifiedTaskDef = actualJob.Def.Tasks[testTaskName]
	modifiedTaskDef.Argv = []string{}
	actualJob.Def.Tasks[testTaskName] = modifiedTaskDef
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "Argv entries are different") {
		t.Errorf("error: comparison logic doesn't work comparing an objects with different task definition args (not empty vs empty): message: %s\n", msg)
	}
	actualJob.Def.Tasks[testTaskName] = savedTaskDef

	// nil args vs non-nil args
	modifiedTaskDef = actualJob.Def.Tasks[testTaskName]
	modifiedTaskDef.Argv = nil
	actualJob.Def.Tasks[testTaskName] = modifiedTaskDef
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "Argv entries are different") {
		t.Errorf("error: comparison logic doesn't work comparing an objects with different task definition args (not empty vs nil): message: %s\n", msg)
	}
	actualJob.Def.Tasks[testTaskName] = savedTaskDef

	// different EnvVars
	// not empty vs empty
	modifiedTaskDef = actualJob.Def.Tasks[testTaskName]
	var modifiedEnvVars map[string]string
	modifiedTaskDef.EnvVars = modifiedEnvVars
	actualJob.Def.Tasks[testTaskName] = modifiedTaskDef
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "EnvVars entries are different") {
		t.Errorf("error: comparison logic doesn't work comparing an objects with different task definition envVars (not empty vs nil): message: %s\n", msg)
	}
	actualJob.Def.Tasks[testTaskName] = savedTaskDef

	// different envVar lengths
	modifiedEnvVars = make(map[string]string)
	modifiedEnvVars[envVar1Name] = "different lengths"
	modifiedTaskDef.EnvVars = modifiedEnvVars
	actualJob.Def.Tasks[testTaskName] = modifiedTaskDef
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "EnvVars entries are different") {
		t.Errorf("error: comparison logic doesn't work comparing an objects with different task definition envVars (different lengths): message: %s\n", msg)
	}
	actualJob.Def.Tasks[testTaskName] = savedTaskDef

	// different values
	modifiedEnvVars[envVar1Name] = "different values"
	modifiedEnvVars[envVar2Name] = actualJob.Def.Tasks[testTaskName].EnvVars[envVar2Name]
	modifiedTaskDef.EnvVars = modifiedEnvVars
	actualJob.Def.Tasks[testTaskName] = modifiedTaskDef
	if ok, msg := origJob.Equal(actualJob); ok || !strings.Contains(msg, "EnvVars entries are different") {
		t.Errorf("error: comparison logic doesn't work comparing an objects with different task definition envVars (different values): message: %s\n", msg)
	}
	actualJob.Def.Tasks[testTaskName] = savedTaskDef

}

func Test_FixedJob(t *testing.T) {
	// use this test to test a specific jobDefinition struct
	origJob := makeFixedSampleJob()

	ValidateSerialization(origJob, BinarySerializer, t)
	ValidateSerialization(origJob, JsonSerializer, t)
}

func Test_SerializeNilJob(t *testing.T) {
	if asByteArray, err := SerializeJobDef(nil, JsonSerializer); err != nil || asByteArray != nil {
		t.Errorf("error: couldn't serialize a nil job. %s, %s\n", err.Error(), string(asByteArray))
	}
	if asByteArray, err := SerializeJobDef(nil, BinarySerializer); err != nil || asByteArray != nil {
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
	envVars[envVar1Name] = "var2Value"
	envVars[envVar2Name] = "var2Value"
	args := []string{"arg1", "arg2"}
	taskDefinition.Argv = args
	jobDef.Tasks[testTaskName] = taskDefinition
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
			ValidateSerialization(job, BinarySerializer, t)
			ValidateSerialization(job, JsonSerializer, t)
			return true
		},

		GopterGenJob(),
	))

	properties.TestingRun(t)

}

func ValidateSerialization(origJob *Job, serializer JobSerializer, t *testing.T) {

	if asByteArray, err := SerializeJobDef(origJob, serializer); err != nil {
		t.Errorf("error: couldn't serialize the fixed job def. %s\n", err.Error())

	} else {
		// deserialize the byte array
		if newJob, err := DeserializeJobDef(asByteArray, serializer); err != nil {
			fmt.Printf("serialize/deserialize test couldn't deserialize object:\n")
			Print(origJob)
			fmt.Printf(fmt.Sprintf("Serialized to:%s\n", string(asByteArray)))
			t.Errorf("error: deserializing the byte Array: %s\n%s\n", string(asByteArray), err.Error())

			// compare the orig and generated task definitions
		} else if ok, msg := origJob.Equal(newJob); !ok {
			fmt.Printf("serialize/deserialize test didn't return equivalent value: %s\n", msg)
			fmt.Printf("original jobDef:\n")
			Print(origJob)
			fmt.Printf(fmt.Sprintf("Serialized to:%s\n", string(asByteArray)))
			fmt.Printf("deserialized to:\n")
			Print(newJob)
			t.Errorf("fail: task definitions are not equal: %s\n", msg)
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
		taskName := fmt.Sprintf("taskName:%s", testhelpers.GenRandomAlphaNumericString(genParams.Rng))
		numArgs := int(genParams.NextUint64() % 5)

		var j int
		var args []string
		for j = 0; j < numArgs; j++ {
			args = append(args, fmt.Sprintf("arg%d:%s", j, testhelpers.GenRandomAlphaNumericString(genParams.Rng)))
		}
		taskDef.Argv = args

		envVarsMap := make(map[string]string)
		numEnvVars := int(genParams.NextUint64() % 5)
		for j = 0; j < numEnvVars; j++ {
			envVarsMap[fmt.Sprintf("env%d", j)] = testhelpers.GenRandomAlphaNumericString(genParams.Rng)
		}
		taskDef.EnvVars = envVarsMap

		timeoutVal := genParams.NextUint64() % 10000
		timeout, _ := time.ParseDuration(fmt.Sprintf("+%ds", timeoutVal))
		taskDef.Timeout = timeout

		taskDef.SnapshotId = fmt.Sprintf("snapShotId:%s", testhelpers.GenRandomAlphaNumericString(genParams.Rng))
		taskDefMap[taskName] = taskDef

	}
	jobDef.Tasks = taskDefMap
	job.Def = jobDef

	return &job
}
