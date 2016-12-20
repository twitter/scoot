package sched

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/leanovate/gopter"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/tests/testhelpers"
)

func GenJob(id string, numTasks int) Job {
	rand := testhelpers.NewRand()
	jobDef := GenRandomJobDef(numTasks, rng)
	//jobDef := GenJobDef(numTasks)

	job := Job{
		Id:  id,
		Def: jobDef,
	}

	return job
}

func GenRandomJob(id string, numTasks int, rng *rand.Rand) Job {
	jobDef := GenRandomJobDef(numTasks, rng)

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

func GenTask() TaskDefinition {
	return TaskDefinition{
		Command: runner.Command{
			Argv: []string{"complete 0"},
		},
	}
}

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

	cmd := runner.Command{SnapshotId: snapshotId, Argv: args, EnvVars: envVarsMap, Timeout: timeout}
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
