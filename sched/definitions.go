package sched

import (
	"github.com/scootdev/scoot/runner"
	"fmt"
	"reflect"
)

// Job is the job Scoot can schedule
type Job struct {
	Id  string
	Def JobDefinition
}

// JobDefinition is the definition the client sent us
type JobDefinition struct {
	JobType string
	Tasks   map[string]TaskDefinition
}

// Task is one task to run
type TaskDefinition struct {
	runner.Command
}

// Status for Job & Tasks
type Status int

const (
	// NotRunning, waiting to be scheduled
	NotStarted Status = iota

	// Currently Scheduled and In Progress Job/Task
	InProgress

	// Successfully Completed Job/Task
	Completed

	// Job was Aborted, Compensating Tasks are being Applied.
	// A RollingBack task has not finished its compensating
	// tasks yet.
	RollingBack

	// Job/Task finished unsuccessfully all compensating actions
	// have been applied.
	RolledBack
)


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
	if argvOk, msg := stringSliceEqual(thisTask.Argv, thatTask.Argv); !argvOk {
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

func stringSliceEqual(expectedSlice, actualSlice []string) (bool, string) {
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

