package sched

import (
	"fmt"
	"github.com/scootdev/scoot/runner"
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

// could not use reflect.DeepEqual(obj1, obj2) - it returns a false negative when the
// args in a task are empty
func JobEqual(job1, job2 *Job) (bool, string) {

	if job1 == job2 {
		return true, ""
	}

	if job1 == nil || job2 == nil {
		return false, fmt.Sprintf("job equals failed, one but not both jobs are nil")
	}

	if job1.Id != job2.Id {
		return false, fmt.Sprintf("job Ids differ: job1 id '%s', job2 id '%s'\n", job1.Id, job2.Id)
	}

	if job1.Def.JobType != job2.Def.JobType {
		return false, fmt.Sprintf("job Types differ: job1 type '%s', job2 type '%s'\n", job1.Def.JobType, job2.Def.JobType)
	}

	map1 := job1.Def.Tasks
	map2 := job2.Def.Tasks
	if len(map1) != len(map2) {
		return false, fmt.Sprintf("task Definitions maps  are different lengths: job1 task map len: %d, job2 task map len: %d\n", len(map1), len(map2))
	}

	for taskName, _ := range map1 {
		task1, _ := map1[taskName]
		task2, foundTask := map2[taskName]
		if !foundTask {
			return false, fmt.Sprintf("job1 taskDef doesn't contain an entry for task : %s from job2\n",taskName)
		}
		if ok, msg := TaskDefinitionEqual(&task1, &task2); !ok {
			return false, msg
		}
	}

	return true, ""
}

func StringMapEqual(map1, map2 map[string]string) (bool, string) {

	if ok := reflect.DeepEqual(map1, map2); !ok {
		return ok, "the maps are not equal\n"
	}

	return true, ""
}

func TaskDefinitionEqual(task1, task2 *TaskDefinition) (bool, string) {
	if argvOk, msg := StringSliceEqual(task1.Argv, task2.Argv); !argvOk {
		return false, "Argv entries are different:" + msg
	}
	if envOk, msg := StringMapEqual(task1.EnvVars, task2.EnvVars); !envOk {
		return false, "EnvVars entries are different:" + msg
	}
	if task1.Timeout.String() != task2.Timeout.String() {
		return false, fmt.Sprintf("Timeout values differ, task1 timeout '%s', task2 timeout '%s'\n", task1.Timeout.String(), task2.Timeout.String())
	}
	if task1.SnapshotId != task2.SnapshotId {
		return false, fmt.Sprintf("Snapshot ids differ, task1 id '%s', task2 timeout '%s'\n", task1.SnapshotId, task2.SnapshotId)
	}

	return true, ""
}

func StringSliceEqual(slice1, slice2 []string) (bool, string) {
	// DeepEqual does not work with nil/empty slices
	if len(slice1) == 0 && len(slice2) == 0 {
		return true, ""
	}

	if ok := reflect.DeepEqual(slice1, slice2); !ok {
		return ok, "the slices are not equal\n"
	}

	return true, ""

}
