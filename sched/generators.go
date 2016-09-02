package sched

import (
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/tests/testhelpers"
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
			Argv: []string{"sleep", "1"},
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
