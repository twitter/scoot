package sched

import (
	"testing"
	//"github.com/leanovate/gopter"
	//"github.com/leanovate/gopter/prop"
	"fmt"
	"time"
)

const serializeType = JsonSerialize


func Test_JobDefCompare(t *testing.T) {
	origJobDef := makeSampleJobDefintion()

	// validate comparison logic
	if ok, msg := taskDefMapEqual(origJobDef.Tasks, origJobDef.Tasks); !ok {
		panic(fmt.Sprintf("error: comparison logic doesn't work comparing an object against itself %s\n", msg))
	}
	if ok, msg := taskDefMapEqual(origJobDef.Tasks, nil); ok {
		panic(fmt.Sprintf("error: comparison logic doesn't work comparing an object against nil %s\n", msg))
	}

}

func Test_NilJobDefinition(t *testing.T) {
	if ok, err := Serialize(nil, serializeType); err == nil {
		panic("error: should have gotten an error from serializing a nil object\n")
		fmt.Printf(string(ok))
	}
}

func Test_FixedJobDefinition(t *testing.T) {
	// use this test to test a specific jobDefinition struct
	origJobDef := makeSampleJobDefintion()


	// serialize the object to a byte array
	if asByteArray, err := Serialize(&origJobDef, serializeType); err != nil {
		panic(fmt.Sprintf("error: couldn't serialize the fixed job def. %s\n", err.Error()))
	} else {
		// deserialize the byte array
		if newJobDef, err := Deserialize(asByteArray, serializeType); err != nil {
			panic(fmt.Sprintf("error: deserializing the byte Array: %s\n", string(asByteArray)) + err.Error())
			// compare the orig and generated job types
		} else if (origJobDef.JobType != newJobDef.JobType) {
			panic(fmt.Sprintf("fail: deserialized job def type doesn't equal original job def, expected %s, got %s\n", origJobDef.JobType, newJobDef.JobType))
			// compare the orig and generated task definitions
		} else if ok, msg := taskDefMapEqual(origJobDef.Tasks, newJobDef.Tasks); !ok {
			panic(fmt.Sprintf("fail: task definitions are not equal: %s\n", msg))
		} else {
			fmt.Printf(fmt.Sprintf("ok=%t\n", ok))
		}
	}
}

func makeSampleJobDefintion() JobDefinition {
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
	//Print(jobDef)

	return jobDef
}

func taskDefMapEqual(expectedMap, actualMap map[string]TaskDefinition) (bool, string) {
	if len(expectedMap) != len(actualMap) {
		return false, fmt.Sprintf("task Definitions maps two different lengths: expected %d, got %d", len(expectedMap), len(actualMap))
	}

	for taskName, _ := range expectedMap {
		def1Task, _ := expectedMap[taskName]
		def2Task, foundTask := actualMap[taskName];
		if !foundTask {
			return false, "actual taskDef doesn't contain the expected entry for key:" + taskName
		}
		if argvOk, msg := stringSliceCompare(def1Task.Argv, def2Task.Argv); !argvOk {
			return false, "Argv entries are different:" + msg
		}
		if envOk, msg := stringMapEqual(def1Task.EnvVars, def2Task.EnvVars); !envOk {
			return false, "EnvVars entries are different:" + msg
		}
		if def1Task.Timeout.String() != def2Task.Timeout.String() {
			fmt.Printf(fmt.Sprintf("expected timeout: %s, actual timeout: %s", def1Task.Timeout.String(), def2Task.Timeout.String()))
			return false, fmt.Sprintf("Timeout values differ, expected %s, got %s", def1Task.Timeout.String(), def2Task.Timeout.String())
		}
		if def1Task.SnapshotId != def2Task.SnapshotId {
			return false, fmt.Sprintf("Snapshot ids differ, expected %s, got %s", def1Task.SnapshotId, def2Task.SnapshotId)
		}
	}

	return true, ""
}

func stringMapEqual(expectedMap, actualMap map[string]string) (bool, string) {
	if len(expectedMap) != len(actualMap) {
		return false, fmt.Sprintf("The 2 maps are different lengths: expected %d, got %d", len(expectedMap), len(actualMap))
	}

	for taskName, entry1 := range expectedMap {
		if entry2, ok := actualMap[taskName]; !ok || entry1 != entry2 {
			return false, fmt.Sprintf("The map entries (expected %s, got %s) for %s have different values.", expectedMap[taskName], actualMap[taskName], taskName)
		}
	}

	return true, ""
}

func stringSliceCompare(expectedSlice, actualSlice []string) (bool, string) {
	if len(expectedSlice) != len(actualSlice) {
		return false, fmt.Sprintf("The 2 slices (expected %s, got %s) are different lengths: expected %d, got %d", expectedSlice, actualSlice, len(expectedSlice), len(actualSlice))
	}

	for j, val1 := range expectedSlice {
		if actualSlice[j] != val1 {
			return false, fmt.Sprintf("Slice entries differ at index %d: expected %s, got %s", j, expectedSlice[j], actualSlice[j])
		}
	}
	return true, ""
}

func Print(jobDef JobDefinition) {
	for taskName, taskDef := range jobDef.Tasks {
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

//func Test_RandomSerializerDeserializer(t *testing.T) {
//	parameters := gopter.DefaultTestParameters()
//	parameters.MinSuccessfulTests = 1000
//	properties := gopter.NewProperties(parameters)
//
//	properties.Property("Serialize xxx", prop.ForAll(
//		func(jobDef JobDefinition) bool {
//			//serialize then deserialized -  should get equal objects
//			if defAsByte, err := Serialize(jobDef); err != nil {
//				return false
//			} else {
//				deserializedJobDef := Deserialize(defAsByte)
//				if jobDef != deserializedJobDef {
//					return false
//				}
//				return true
//			}
//
//		},
//
//		genJobDef(),
//	))
//
//	properties.TestingRun(t)
//
//}
//
//func genJobDef() gopter.Gen {
//	return func(genParams *gopter.GenParameters) *gopter.GenResult {
//		jobDef := genJobDefFromParams(genParams)
//		genResult := gopter.NewGenResult(jobDef, gopter.NoShrinker)
//		return genResult
//	}
//}
//
//func genJobDefFromParams(genParams *gopter.GenParameters) JobDefinition {
//	jobDef := JobDefinition{}
//	jobDef.JobType = genString(genParams)
//
//	//number of tasks to run in this saga
//	numTasks := int(genParams.NextUint64() % 100)
//	jobDef.Tasks = make(map[string]TaskDefinition)
//
//	for i := 0; i < numTasks; i++ {
//		taskDef := TaskDefinition{}
//		taskName := genString(genParams)
//		jobDef.Tasks[taskName] = taskDef
//		numArgs := int(genParams.NextInt64()) % 10
//		var j int8
//		for j = 0 ; j < numArgs; j++ {
//			taskDef.Argv[j] = genString(genParams)
//		}
//
//		numEnvVars := int(genParams.NextInt64()) %20
//		for j = 0 ; j < numEnvVars; j++ {
//			taskDef.EnvVars[j] = genString(genParams)
//		}
//
//		taskDef.Timeout = genParams.NextUint64()
//
//		taskDef.SnapshotId = genString(genParams)
//
//	}
//
//	return jobDef
//}
//
//// generate a random string for the type
//func genString(genParams *gopter.GenParameters) string {
//	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
//	length := int(genParams.NextUint64()%20) + 1
//	result := make([]byte, length)
//	for i := 0; i < length; i++ {
//		result[i] = chars[genParams.Rng.Intn(len(chars))]
//	}
//
//	return string(result)
//}
