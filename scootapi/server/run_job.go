package server

import (
	"fmt"
	"github.com/nu7hatch/gouuid"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

// Implementation of the RunJob API
func runJob(scheduler scheduler.Scheduler, def *scoot.JobDefinition) (*scoot.JobId, error) {

	jobDef, err := thriftJobToScoot(def)
	// TODO: change to return scoot.NewInvalidRequest()
	if err != nil {
		return nil, err
	}

	err = validateJob(jobDef)
	if err != nil {
		return nil, err
	}

	job := sched.Job{
		Id:  generateJobId(),
		Def: jobDef,
	}

	err = scheduler.ScheduleJob(job)

	if err != nil {
		return nil, scoot.NewCanNotScheduleNow()
	}

	return &scoot.JobId{ID: job.Id}, nil
}

func generateJobId() string {
	id, err := uuid.NewV4()
	for err != nil {
		id, err = uuid.NewV4()
	}

	return id.String()
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
			task.SnapshotId = *t.SnapshotId
		}
		result.Tasks[taskId] = task
	}

	if def.JobType != nil {
		result.JobType = def.JobType.String()
	}

	return result, nil
}

// Validate a job, returning an *InvalidJobRequest if invalid.
func validateJob(job sched.JobDefinition) error {
	fmt.Println(len(job.Tasks))
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
