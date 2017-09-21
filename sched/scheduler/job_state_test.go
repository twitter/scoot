package scheduler

import (
	"testing"

	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/tests/testhelpers"
)

func Test_GetUnscheduledTasks_ReturnsAllUnscheduledTasks(t *testing.T) {
	job := sched.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 1)
	jobAsBytes, _ := job.Serialize()

	saga, _ := sagalogs.MakeInMemorySagaCoordinator().MakeSaga(job.Id, jobAsBytes)
	jobState := newJobState(&job, saga, nil)

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
	for _, task := range job.Def.Tasks {
		saga.StartTask(task.TaskID, nil)
	}
	jobState := newJobState(&job, saga, nil)

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
	for _, task := range job.Def.Tasks {
		saga.StartTask(task.TaskID, nil)
		saga.EndTask(task.TaskID, nil)
	}
	jobState := newJobState(&job, saga, nil)

	tasks := jobState.getUnScheduledTasks()
	if len(tasks) != 0 {
		t.Errorf("Expected all Tasks to be completed")
	}
}
