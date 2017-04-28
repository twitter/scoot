package server

import (
	"fmt"
	"time"

	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

// Implementation of the RunJob API
func runJob(scheduler scheduler.Scheduler, def *scoot.JobDefinition, stat stats.StatsReceiver) (*scoot.JobId, error) {
	stat.Counter("runJobRequestsCounter").Inc(1)

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
		return nil, scoot.NewCanNotScheduleNow()
	}

	return &scoot.JobId{ID: id}, nil
}

// Translates thrift job definition message to scoot domain object
func thriftJobToScoot(def *scoot.JobDefinition) (result sched.JobDefinition, err error) {
	if def == nil {
		return result, fmt.Errorf("nil job definition")
	}
	result.Tasks = make(map[string]sched.TaskDefinition)

	for taskId, t := range def.Tasks {
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
		if t.TimeoutMs != nil {
			task.Command.Timeout = time.Duration(*t.TimeoutMs) * time.Millisecond
		} else if def.TimeoutMs != nil {
			task.Command.Timeout = time.Duration(*def.TimeoutMs) * time.Millisecond
		}
		t.TaskId = &taskId
		result.Tasks[taskId] = task
	}

	if def.JobType != nil {
		result.JobType = def.JobType.String()
	}

	return result, nil
}

// Validate a job, returning an *InvalidJobRequest if invalid.
func validateJob(job sched.JobDefinition) error {
	if len(job.Tasks) == 0 {
		return NewInvalidJobRequest("invalid job. Must have at least 1 task; was empty")
	}
	for id, task := range job.Tasks {
		if id == "" {
			return NewInvalidJobRequest("invalid task id \"\".")
		}
		if len(task.Command.Argv) == 0 {
			return NewInvalidJobRequest("invalid task.Command.Argv. Must have at least one argument; was empty")
		}
	}
	return nil
}
