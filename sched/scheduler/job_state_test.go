package scheduler

import (
	"github.com/scootdev/scoot/saga/sagalogs"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/tests/testhelpers"
	"testing"
)

func Test_GetUnscheduledTasks_ReturnsAllUnscheduledTasks(t *testing.T) {
	job := sched.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 1)
	jobAsBytes, _ := job.Serialize()

	saga, _ := sagalogs.MakeInMemorySagaCoordinator().MakeSaga(job.Id, jobAsBytes)
	jobState := newJobState(&job, saga)

	tasks := jobState.getUnScheduledTasks()

	if len(tasks) != len(job.Def.Tasks) {
		t.Errorf("Expected all unscheduled tasks to be returned")
	}
}

func Test_NewJobState_PreviousProgress_StartedTasks(t *testing.T) {
	job := sched.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 1)
	jobAsBytes, _ := job.Serialize()

	// Mark all tasks as started, then create jobState
	saga, _ := sagalogs.MakeInMemorySagaCoordinator().MakeSaga(job.Id, jobAsBytes)
	for taskId, _ := range job.Def.Tasks {
		saga.StartTask(taskId, nil)
	}
	jobState := newJobState(&job, saga)

	tasks := jobState.getUnScheduledTasks()
	if len(tasks) != len(job.Def.Tasks) {
		t.Errorf("Expected all Tasks to be unschedled")
	}
}

func Test_NewJobState_PreviousProgress_CompletedTasks(t *testing.T) {
	job := sched.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 1)
	jobAsBytes, _ := job.Serialize()

	// Mark all tasks as completed, then create jobState
	saga, _ := sagalogs.MakeInMemorySagaCoordinator().MakeSaga(job.Id, jobAsBytes)
	for taskId, _ := range job.Def.Tasks {
		saga.StartTask(taskId, nil)
		saga.EndTask(taskId, nil)
	}
	jobState := newJobState(&job, saga)

	tasks := jobState.getUnScheduledTasks()
	if len(tasks) != 0 {
		t.Errorf("Expected all Tasks to be unschedled")
	}
}
