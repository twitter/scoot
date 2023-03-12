package thrift

import (
	"fmt"
	"time"

	"github.com/wisechengyi/scoot/common/stats"
	"github.com/wisechengyi/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/wisechengyi/scoot/scheduler/domain"
	"github.com/wisechengyi/scoot/scheduler/server"
)

// Implementation of the RunJob API
func RunJob(scheduler server.Scheduler, def *scoot.JobDefinition, stat stats.StatsReceiver) (*scoot.JobId, error) {
	jobDef, err := thriftJobToScoot(def)
	// TODO: change to return scoot.NewInvalidRequest()
	if err != nil {
		return nil, err
	}

	err = domain.ValidateJob(jobDef)
	if err != nil {
		return nil, NewInvalidJobRequest(err.Error())
	}

	id, err := scheduler.ScheduleJob(jobDef)

	if err != nil {
		return nil, err //TODO: use or delete scoot.NewCanNotScheduleNow()
	}

	return &scoot.JobId{ID: id}, nil
}

// Translates thrift job definition message to scoot domain object
func thriftJobToScoot(def *scoot.JobDefinition) (result domain.JobDefinition, err error) {
	if def == nil {
		return result, fmt.Errorf("nil job definition")
	}
	result.Tasks = []domain.TaskDefinition{}

	for _, t := range def.Tasks {
		var task domain.TaskDefinition
		if t == nil {
			return result, fmt.Errorf("nil task definition")
		}
		if t.Command == nil {
			return result, fmt.Errorf("nil command")
		}
		task.Command.Argv = t.Command.Argv
		task.Command.EnvVars = make(map[string]string)
		for k, v := range t.Command.EnvVars {
			task.Command.EnvVars[k] = v
		}
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
		result.Requestor = *def.Requestor
	}
	if def.Priority != nil {
		result.Priority = domain.Priority(*def.Priority)
	}

	return result, nil
}
