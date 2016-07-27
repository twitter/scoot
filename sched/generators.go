package sched

import (
	"fmt"
	"github.com/leanovate/gopter"
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
		Command: Command{
			Argv: []string{"cmd1", "arg1", "arg2"},
		},
	}
}

// Randomly generates an Id that is valid for
// use as a sagaId or taskId
func GenJobId() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
		length := (genParams.Rng.Intn(20) + 1)
		result := make([]byte, length)
		for i := 0; i < length; i++ {
			result[i] = chars[genParams.Rng.Intn(len(chars))]
		}

		return gopter.NewGenResult(string(result), gopter.NoShrinker)
	}
}
