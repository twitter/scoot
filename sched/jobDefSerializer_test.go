package sched

import (
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/scootdev/scoot/tests/testhelpers"
	"testing"
	"time"
	"reflect"
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

func Test_NilJob(t *testing.T) {
	if ok, err := Serialize(nil, serializer); err == nil {
		t.Errorf("error: did not get an error from serializing a nil object, instead got %s\n", ok)
	}
}

func Test_FixedJob(t *testing.T) {
	// use this test to test a specific jobDefinition struct
	origJob := makeSampleJob()

	// serialize the object to a byte array
	if asByteArray, err := Serialize(&origJob, serializer); err != nil {
		t.Errorf("error: couldn't serialize the fixed job def. %s\n", err.Error())

	} else {
		// deserialize the byte array
		if newJob, err := Deserialize(asByteArray, serializer); err != nil {
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

// could not use reflect.DeepCopy(obj1, obj2) - it returns a false negative when the
// envVars or args in a task are empty
func (this *Job) Equal(thatJobDef Job) (bool, string) {

	if (*this).Id != thatJobDef.Id {
		return false, fmt.Sprintf("job Ids differ: expected %s, got %s\n", this.Id, thatJobDef.Id)
	}


	if (*this).Def.JobType != thatJobDef.Def.JobType {
		return false, fmt.Sprintf("job Types differ: expected %s, got %s\n", this.Def.JobType, thatJobDef.Def.JobType)
	}

	expectedMap := (*this).Def.Tasks
	actualMap := thatJobDef.Def.Tasks
	if len(expectedMap) != len(actualMap) {
		return false, fmt.Sprintf("task Definitions maps two different lengths: expected %d, got %d", len(expectedMap), len(actualMap))
	}

	for taskName, _ := range expectedMap {
		thisTask, _ := expectedMap[taskName]
		thatTask, foundTask := actualMap[taskName]
		if !foundTask {
			return false, "actual taskDef doesn't contain the expected entry for key:" + taskName
		}
		if ok, msg := thisTask.Equal(thatTask); !ok {
			return false, msg
		}
	}

	return true, ""
}

func stringMapEqual(expectedMap, actualMap map[string]string) (bool, string) {
	if len(expectedMap) != len(actualMap) {
		return false, fmt.Sprintf("The 2 maps are different lengths: expected %d, got %d", len(expectedMap), len(actualMap))
	}

	if ok := reflect.DeepEqual(expectedMap, actualMap); !ok {
		return ok, "the maps are not equal"
	}

	return true, ""
}

func (thisTask *TaskDefinition) Equal(thatTask TaskDefinition) (bool, string) {
	if argvOk, msg := stringSliceCompare(thisTask.Argv, thatTask.Argv); !argvOk {
		return false, "Argv entries are different:" + msg
	}
	if envOk, msg := stringMapEqual(thisTask.EnvVars, thatTask.EnvVars); !envOk {
		return false, "EnvVars entries are different:" + msg
	}
	if thisTask.Timeout.String() != thatTask.Timeout.String() {
		fmt.Printf(fmt.Sprintf("expected timeout: %s, actual timeout: %s", thisTask.Timeout.String(), thatTask.Timeout.String()))
		return false, fmt.Sprintf("Timeout values differ, expected %s, got %s", thisTask.Timeout.String(), thatTask.Timeout.String())
	}
	if thisTask.SnapshotId != thatTask.SnapshotId {
		return false, fmt.Sprintf("Snapshot ids differ, expected %s, got %s", thisTask.SnapshotId, thatTask.SnapshotId)
	}

	return true, ""
}

func stringSliceCompare(expectedSlice, actualSlice []string) (bool, string) {
	if len(expectedSlice) != len(actualSlice) {
		return false, fmt.Sprintf("The 2 slices (expected %s, got %s) are different lengths: expected %d, got %d", expectedSlice, actualSlice, len(expectedSlice), len(actualSlice))
	}

	// DeepCopy does not work with nil slices
	if expectedSlice == nil && actualSlice == nil {
		return true, ""
	}

	// DeepCopy does not work with slices of 0 length
	if len(expectedSlice) == 0 && len(actualSlice) == 0 {
		return true, ""
	}

	if ok := reflect.DeepEqual(expectedSlice, actualSlice); !ok {
		return ok, "the slices are not equal"
	}

	return true, ""
}

func Print(job Job) {
	fmt.Printf(fmt.Sprintf("job id:%s\n",job.Id))
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
			if defAsByte, err := Serialize(&job, serializer); err != nil {
				return false
			} else {
				deserializedJob, err := Deserialize(defAsByte, serializer)
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

		genJob(),
	))

	properties.TestingRun(t)

}

func genJob() gopter.Gen {
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
