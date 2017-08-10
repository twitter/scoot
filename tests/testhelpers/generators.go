package testhelpers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
)

// generates a new random number seeded with
func NewRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

// Test Helpers that are useful for Generating random Scoot Api Structs
// To help with testing the Scoot API and Scheduler

// Generates a scoot.JobDefinition with numTasks tasks (or random if numTasks == -1)
func GenJobDefinition(rng *rand.Rand, numTasks int, snapshotID string) *scoot.JobDefinition {
	def := scoot.NewJobDefinition()
	def.Tasks = make([]*scoot.TaskDefinition, 0)

	if numTasks == -1 {
		numTasks = rng.Intn(10) + 1
	}

	for i := 0; i < numTasks; i++ {
		taskId := fmt.Sprintf("%d%v", i, GenTaskId(rng))
		taskDef := GenTask(rng, taskId, snapshotID)
		def.Tasks = append(def.Tasks, taskDef)
	}

	return def
}

// Generates a scoot.TaskDefinition
// TODO: actually make more realistic
func GenTask(rng *rand.Rand, taskID, snapshotID string) *scoot.TaskDefinition {

	cmd := scoot.NewCommand()
	cmd.Argv = []string{execers.UseSimExecerArg, "sleep 100", "complete 0"}

	taskDef := scoot.NewTaskDefinition()
	taskDef.Command = cmd
	taskDef.TaskId = &taskID
	taskDef.SnapshotId = &snapshotID

	return taskDef
}

// Generates a valid random JobId
func GenJobId(rng *rand.Rand) string {
	return GenRandomAlphaNumericString(rng)
}

// Generates a valid random TaskID
func GenTaskId(rng *rand.Rand) string {
	return GenRandomAlphaNumericString(rng)
}

// Generates an AlphaNumericString of random length (0, 21]
func GenRandomAlphaNumericString(rng *rand.Rand) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	length := rng.Intn(20) + 1
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[rng.Intn(len(chars))]
	}

	return string(result)
}
