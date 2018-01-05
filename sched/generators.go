package sched

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/leanovate/gopter"

	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/tests/testhelpers"
)

// Generates a Random Job with the specified Id and number of Tasks
func GenJob(id string, numTasks int) Job {
	rand := testhelpers.NewRand()
	jobDef := GenRandomJobDef(numTasks, rand)

	job := Job{
		Id:  id,
		Def: *jobDef,
	}

	return job
}

// Generates a Random Job, using the supplied Rand
// with the specified Id and number of Tasks
func GenRandomJob(id string, numTasks int, rng *rand.Rand) Job {
	jobDef := GenRandomJobDef(numTasks, rng)
	for taskId, task := range jobDef.Tasks {
		var newTask TaskDefinition
		newTask.Command = task.Command
		newTask.JobID = id
		jobDef.Tasks[taskId] = newTask
	}

	job := Job{
		Id:  id,
		Def: *jobDef,
	}

	return job
}

// Generates a Random JobDefintion with the specified number of tasks
func GenJobDef(numTasks int) JobDefinition {
	rand := testhelpers.NewRand()
	return *GenRandomJobDef(numTasks, rand)
}

// Generates a Random Job Definition, using the supplied Rand
// with the specified number of Tasks
func GenRandomJobDef(numTasks int, rng *rand.Rand) *JobDefinition {
	tag := fmt.Sprintf("tag:%s", testhelpers.GenRandomAlphaNumericString(rng))
	jobDef := JobDefinition{
		JobType: fmt.Sprintf("jobType:%s", testhelpers.GenRandomAlphaNumericString(rng)),
		Tasks:   make([]TaskDefinition, 0),
		Tag:     tag,
	}

	// Generate tasks
	seen := map[string]bool{}
	for i := 0; i < numTasks; i++ {
		task := GenRandomTask(rng, tag)
		for {
			if _, ok := seen[task.TaskID]; ok {
				task.TaskID = fmt.Sprintf("taskName:%s", testhelpers.GenRandomAlphaNumericString(rng))
			} else {
				break
			}
		}
		seen[task.TaskID] = true
		jobDef.Tasks = append(jobDef.Tasks, task)
	}

	return &jobDef
}

// Generates a Random TaskDefinition
func GenTask() TaskDefinition {
	rand := testhelpers.NewRand()
	return GenRandomTask(rand, fmt.Sprintf("tag:%s", testhelpers.GenRandomAlphaNumericString(rand)))
}

// Generates a Random TaskDefinition, using the supplied Rand
func GenRandomTask(rng *rand.Rand, tag string) TaskDefinition {
	snapshotId := fmt.Sprintf("snapShotId:%s", testhelpers.GenRandomAlphaNumericString(rng))
	taskId := fmt.Sprintf("taskId:%s", testhelpers.GenRandomAlphaNumericString(rng))
	numArgs := rng.Intn(5)
	var j int
	var args []string = []string{}
	for j = 0; j < numArgs; j++ {
		args = append(args, fmt.Sprintf("arg%d:%s", j, testhelpers.GenRandomAlphaNumericString(rng)))
	}

	var envVarsMap map[string]string = make(map[string]string)
	numEnvVars := rng.Intn(5)
	for j = 0; j < numEnvVars; j++ {
		envVarsMap[fmt.Sprintf("env%d", j)] = testhelpers.GenRandomAlphaNumericString(rng)
	}

	timeout := 10 * time.Second //Arbitrary value to support pausing sim execer tests.
	cmd := runner.Command{
		SnapshotID: snapshotId,
		Argv:       args,
		EnvVars:    envVarsMap,
		Timeout:    timeout,
		LogTags: tags.LogTags{
			TaskID: taskId,
			Tag:    tag,
		},
	}

	return TaskDefinition{cmd}
}

// Randomly generates an Id that is valid for
// use as a sagaId or taskId
func GenJobId() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		result := testhelpers.GenRandomAlphaNumericString(genParams.Rng)
		return gopter.NewGenResult(string(result), gopter.NoShrinker)
	}
}

// Wrapper function Generates a Job of Property Based Tests
func GopterGenJob() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		numTasks := genParams.Rng.Intn(10)
		jobId := testhelpers.GenRandomAlphaNumericString(genParams.Rng)
		job := GenRandomJob(jobId, numTasks, genParams.Rng)

		genResult := gopter.NewGenResult(&job, gopter.NoShrinker)
		return genResult
	}
}

// Wrappper function that Generates a JobDefinition for Property Based Tests
func GopterGenJobDef() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		jobId := testhelpers.GenRandomAlphaNumericString(genParams.Rng)
		numTasks := genParams.Rng.Intn(10)
		job := GenRandomJob(jobId, numTasks, genParams.Rng)
		genResult := gopter.NewGenResult(job.Def, gopter.NoShrinker)
		return genResult
	}
}
