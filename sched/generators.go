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

	job := Job{
		Id:  id,
		Def: jobDef,
	}

	return job
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

	var domainJob = Job{}
	domainJob.Id = thriftJob.ID
	sagaLogJobDef := thriftJob.JobDefinition

	var schedJobDef = JobDefinition{}
	schedJobDef.JobType = sagaLogJobDef.JobType
	schedJobDef.Tasks = make(map[string]TaskDefinition)
	for taskName, sagaLogTaskDefTask := range sagaLogJobDef.Tasks {
		var argvs []string
		argvs = sagaLogTaskDefTask.Command.Argv
		var envVars map[string]string
		envVars = sagaLogTaskDefTask.Command.EnvVars
		timeout := time.Duration(sagaLogTaskDefTask.Command.Timeout)
		snapshotId := sagaLogTaskDefTask.Command.SnapshotId
		command := runner.NewCommand(argvs, envVars, timeout, snapshotId)
		schedJobDef.Tasks[taskName] = TaskDefinition{*command}
	}
	domainJob.Def = schedJobDef
	return &domainJob
}

func makeThriftJobFromDomainJob(domainJob *Job) (*schedthrift.Job, error) {
	if domainJob == nil {
		return nil, nil
	}

	// allocate a thrift Job for saga log
	thriftJob := schedthrift.NewJob()

	// copy the sched JobDefinition properties to the thrift jobDefinition structure
	(*thriftJob).ID = (*domainJob).Id

	internalJobDef := schedthrift.NewJobDefinition()

	jobDef := domainJob.Def
	internalJobDef.JobType = jobDef.JobType
	internalJobDef.Tasks = make(map[string]*schedthrift.TaskDefinition)
	for taskName, taskDef := range jobDef.Tasks {
		internalTaskDef := schedthrift.NewTaskDefinition()
		internalTaskDef.Command = schedthrift.NewCommand()
		internalTaskDef.Command.Argv = taskDef.Argv
		internalTaskDef.Command.EnvVars = taskDef.EnvVars
		internalTaskDef.Command.SnapshotId = taskDef.SnapshotId
		internalTaskDef.Command.Timeout = taskDef.Timeout.Nanoseconds()
		internalJobDef.Tasks[taskName] = internalTaskDef
	}
	thriftJob.JobDefinition = internalJobDef

	return thriftJob, nil

}
