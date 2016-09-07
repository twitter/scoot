package sched

import (
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/scootdev/scoot/tests/testhelpers"
	"testing"
	"time"
)

var serializer = BinarySerializer // options are: JsonSerializer or BinarySerializer

func Test_JobCompare(t *testing.T) {
	origJob := makeSampleJob()

	// validate comparison logic
	if ok, msg := origJob.Equal(origJob); !ok {
		t.Errorf("error: comparison logic doesn't work comparing an object against itself. %s\n", msg)
	}
	var actualJob Job
	if ok, msg := origJob.Equal(actualJob); ok {
		t.Errorf("error: comparison logic doesn't work comparing an object against nil. %s\n", msg)
	}
}

func Test_FixedJob(t *testing.T) {
	// use this test to test a specific jobDefinition struct
	origJob := makeSampleJob()

	// serialize the object to a byte array
	if asByteArray, err := origJob.SerializeJobDef(serializer); err != nil {
		t.Errorf("error: couldn't serialize the fixed job def. %s\n", err.Error())

	} else {
		// deserialize the byte array
		if newJob, err := DeserializeJobDef(asByteArray, serializer); err != nil {
			t.Errorf("error: deserializing the byte Array: %s\n%s\n", string(asByteArray), err.Error())

			// compare the orig and generated task definitions
		} else if ok, msg := origJob.Equal(*newJob); !ok {
			t.Errorf("fail: task definitions are not equal: %s\n", msg)
		}
	}
}

func makeSampleJob() Job {
	job := Job{}
	job.Id = "jobID"
	jobDef := JobDefinition{}
	jobDef.Tasks = make(map[string]TaskDefinition)
	jobDef.JobType = "jobTypeVal"
	taskDefinition := TaskDefinition{}
	taskDefinition.SnapshotId = "snapshotIdVal"
	tx, _ := time.ParseDuration("+3s")
	taskDefinition.Timeout = tx
	envVars := make(map[string]string)
	taskDefinition.EnvVars = envVars
	envVars["var1"] = "var1Value"
	envVars["var2"] = "var2Value"
	args := []string{"arg1", "arg2"}
	taskDefinition.Argv = args
	jobDef.Tasks["task0"] = taskDefinition
	//Print(jobDef)  -I enable this for debugging
	job.Def = jobDef

	return job
}

func Print(job Job) {
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
		func(job Job) bool {
			//serialize then deserialized -  should get equal objects
			if defAsByte, err := job.SerializeJobDef(serializer); err != nil {
				return false
			} else {
				deserializedJob, err := DeserializeJobDef(defAsByte, serializer)
				if err != nil {
					fmt.Printf("serialize/deserialize test couldn't deserialize object:\n")
					Print(job)
					fmt.Printf(fmt.Sprintf("Serialized to:%s\n", string(defAsByte)))
					return false
				}
				if ok, msg := job.Equal(*deserializedJob); !ok {
					fmt.Printf("serialize/deserialize test didn't return equivalent value: %s\n", msg)
					fmt.Printf("original jobDef:\n")
					Print(job)
					fmt.Printf(fmt.Sprintf("Serialized to:%s\n", string(defAsByte)))
					fmt.Printf("deserialized to:\n")
					Print(*deserializedJob)
					return false
				}
				return true
			}

		},

		GopterGenJob(),
	))

	properties.TestingRun(t)

}

func GopterGenJob() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		jobDef := genJobFromParams(genParams)
		//Print(jobDef)
		genResult := gopter.NewGenResult(jobDef, gopter.NoShrinker)
		return genResult
	}
}

func genJobFromParams(genParams *gopter.GenParameters) Job {

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

	return job
}
