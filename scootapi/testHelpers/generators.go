package testHelpers

import (
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"math/rand"
)

// Test Helpers that are useful for Generating random Scoot Api Structs
// To help with testing the Scoot API and Scheduler

// Generates a scoot.JobDefinition
func GenJobDefinition(rng *rand.Rand) *scoot.JobDefinition {

	def := scoot.NewJobDefinition()
	def.Tasks = make(map[string]*scoot.TaskDefinition)

	numTasks := rng.Intn(100) + 1
	for i := 0; i < numTasks; i++ {
		taskId := GenTaskId(rng)
		taskDef := GenTask(rng)

		def.Tasks[taskId] = taskDef
	}

	return def
}

// Generates a scoot.TaskDefinition
// TODO: actually make more realistic
func GenTask(rng *rand.Rand) *scoot.TaskDefinition {

	cmd := scoot.NewCommand()
	cmd.Argv = []string{"cmd1", "arg1", "arg2"}

	taskDef := scoot.NewTaskDefinition()
	taskDef.Command = cmd

	return taskDef
}

// Generates a valid random TaskID
func GenTaskId(rng *rand.Rand) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	length := rng.Intn(20) + 1
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[rng.Intn(len(chars))]
	}

	return string(result)
}
