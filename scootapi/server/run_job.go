package server

import (
	"fmt"
	"time"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/sched/scheduler"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
)

// Implementation of the RunJob API
func runJob(scheduler scheduler.Scheduler, def *scoot.JobDefinition, stat stats.StatsReceiver) (*scoot.JobId, error) {

	jobDef, err := thriftJobToScoot(def)
	// TODO: change to return scoot.NewInvalidRequest()
	if err != nil {
		return nil, err
	}

	err = validateJob(jobDef)
	if err != nil {
		return nil, err
	}

	id, err := scheduler.ScheduleJob(jobDef)

	if err != nil {
		return nil, err //TODO: use or delete scoot.NewCanNotScheduleNow()
	}

	return &scoot.JobId{ID: id}, nil
}

// Translates thrift job definition message to scoot domain object
func thriftJobToScoot(def *scoot.JobDefinition) (result sched.JobDefinition, err error) {
	if def == nil {
		return result, fmt.Errorf("nil job definition")
	}
	result.Tasks = []sched.TaskDefinition{}

	for _, t := range def.Tasks {
		var task sched.TaskDefinition
		if t == nil {
			return result, fmt.Errorf("nil task definition")
		}
		if t.Command == nil {
			return result, fmt.Errorf("nil command")
		}
		task.Command.Argv = t.Command.Argv
		if t.SnapshotId != nil {
			task.SnapshotID = *t.SnapshotId
		}
		if t.TimeoutMs != nil && *t.TimeoutMs > 0 {
			task.Command.Timeout = time.Duration(*t.TimeoutMs) * time.Millisecond
		} else if def.DefaultTaskTimeoutMs != nil {
			task.Command.Timeout = time.Duration(*def.DefaultTaskTimeoutMs) * time.Millisecond
		}
		if t.TaskId == nil {
			return result, fmt.Errorf("nil taskId")
		}
		task.TaskID = *t.TaskId

		result.Tasks = append(result.Tasks, task)
	}

	if def.Tag != nil {
		result.Tag = *def.Tag
	}
	if def.Basis != nil {
		result.Basis = *def.Basis
	}
	if def.JobType != nil {
		result.JobType = *def.JobType
	}
	if def.Requestor != nil {
		result.Requestor = *def.JobType
	}
	if def.Priority != nil {
		result.Priority = sched.Priority(*def.Priority)
	}

	return result, nil
}

// Validate a job, returning an *InvalidJobRequest if invalid.
func validateJob(job sched.JobDefinition) error {
	if len(job.Tasks) == 0 {
		return NewInvalidJobRequest("invalid job. Must have at least 1 task; was empty")
	}
	for _, task := range job.Tasks {
		if task.TaskID == "" {
			return NewInvalidJobRequest("invalid task id \"\".")
		}
		if len(task.Command.Argv) == 0 {
			return NewInvalidJobRequest("invalid task.Command.Argv. Must have at least one argument; was empty")
		}
	}
	return nil
}
