package scheduler

import (
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/tests/testhelpers"
	"testing"
)

func Test_GetUnscheduledTasks_ReturnsAllUnscheduledTasks(t *testing.T) {
	job := sched.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 1)
	jobState := NewJobState(job, nil)

	tasks := jobState.GetUnScheduledTasks()

	if len(tasks) != len(job.Def.Tasks) {
		t.Errorf("Expected all unscheduled tasks to be returned")
	}
}
