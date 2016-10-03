package sched

import (
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched/gen-go/schedthrift"
	"github.com/scootdev/scoot/tests/testhelpers"
	"time"
)

func GenJob(id string, numTasks int) Job {
	jobDef := GenJobDef(numTasks)

	job := Job{
		Id:  id,
		Def: jobDef,
	}

	return job
}

func GenJobDef(numTasks int) JobDefinition {
	jobDef := JobDefinition{
		JobType: "testJob",
		Tasks:   make(map[string]TaskDefinition),
	}

	// Generate tasks
	for i := 0; i < numTasks; i++ {
		task := GenTask()
		taskId := fmt.Sprintf("Task%v", i)
		jobDef.Tasks[taskId] = task
	}

	return jobDef
}

func GenTask() TaskDefinition {
	return TaskDefinition{
		Command: runner.Command{
			Argv: []string{"complete 0"},
		},
	}
}

// Randomly generates an Id that is valid for
// use as a sagaId or taskId
func GenJobId() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		result := testhelpers.GenRandomAlphaNumericString(genParams.Rng)
		return gopter.NewGenResult(string(result), gopter.NoShrinker)
	}
}

func makeDomainJobFromThriftJob(thriftJob *schedthrift.Job) *Job {

	if thriftJob == nil {
		return nil
	}

	thriftJobDef := thriftJob.JobDefinition

	domainTasks := make(map[string]TaskDefinition)
	for taskName, task := range thriftJobDef.Tasks {
		cmd := task.Command
		command := runner.NewCommand(cmd.Argv, cmd.EnvVars, time.Duration(cmd.Timeout), cmd.SnapshotId)
		domainTasks[taskName] = TaskDefinition{*command}
	}

	domainJobDef := JobDefinition{JobType: thriftJobDef.JobType, Tasks: domainTasks}

	return &Job{Id: thriftJob.ID, Def: domainJobDef}
}

func makeThriftJobFromDomainJob(domainJob *Job) (*schedthrift.Job, error) {
	if domainJob == nil {
		return nil, nil
	}

	thriftTasks := make(map[string]*schedthrift.TaskDefinition)
	for taskName, domainTask := range domainJob.Def.Tasks {
		cmd := schedthrift.Command{Argv: domainTask.Argv, EnvVars: domainTask.EnvVars, Timeout: int64(domainTask.Timeout), SnapshotId: domainTask.SnapshotId}
		thriftTask := schedthrift.TaskDefinition{Command: &cmd}
		thriftTasks[taskName] = &thriftTask
	}
	thriftJobDefinition := schedthrift.JobDefinition{JobType: (*domainJob).Def.JobType, Tasks: thriftTasks}
	thriftJob := schedthrift.Job{ID: domainJob.Id, JobDefinition: &thriftJobDefinition}

	return &thriftJob, nil

}
