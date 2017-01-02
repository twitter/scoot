package sched

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/leanovate/gopter"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/tests/testhelpers"
)

// Generates a Random Job with the specified Id and number of Tasks
func GenJob(id string, numTasks int) Job {
	rand := testhelpers.NewRand()
	jobDef := GenRandomJobDef(numTasks, rand)

	job := Job{
		Id:  id,
		Def: jobDef,
	}

	return job
}

// Generates a Random Job, using the supplied Rand
// with the specified Id and number of Tasks
func GenRandomJob(id string, numTasks int, rng *rand.Rand) Job {
	jobDef := GenRandomJobDef(numTasks, rng)

	job := Job{
		Id:  id,
		Def: jobDef,
	}

	return job
}

// Generates a Random JobDefintion with the specified number of tasks
func GenJobDef(numTasks int) JobDefinition {
	rand := testhelpers.NewRand()
	return GenRandomJobDef(numTasks, rand)
}

// Generates a Random Job Definition, using the supplied Rand
// with the specified number of Tasks
func GenRandomJobDef(numTasks int, rng *rand.Rand) JobDefinition {
	jobDef := JobDefinition{
		JobType: fmt.Sprintf("jobType:%s", testhelpers.GenRandomAlphaNumericString(rng)),
		Tasks:   make(map[string]TaskDefinition),
	}

	// Generate tasks
	for i := 0; i < numTasks; i++ {
		task := GenRandomTask(rng)
		taskId := fmt.Sprintf("taskName:%s", testhelpers.GenRandomAlphaNumericString(rng))
		jobDef.Tasks[taskId] = task
	}

	return jobDef
}

// Generates a Random TaskDefinition
func GenTask() TaskDefinition {
	rand := testhelpers.NewRand()
	return GenRandomTask(rand)
}

// Generates a Random TaskDefinition, using the supplied Rand
func GenRandomTask(rng *rand.Rand) TaskDefinition {
	snapshotId := fmt.Sprintf("snapShotId:%s", testhelpers.GenRandomAlphaNumericString(rng))
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

	timeout := time.Duration(rng.Int63n(1000))
	cmd := runner.Command{
		SnapshotID: snapshotId,
		Argv:       args,
		EnvVars:    envVarsMap,
		Timeout:    timeout,
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
