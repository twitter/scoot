package memory_test

import (
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue/memory"
	"testing"
)

func TestBadDef(t *testing.T) {
	q, _ := memory.NewSimpleQueue()
	job := sched.Job{}
	_, err := q.Enqueue(job)
	if err == nil {
		t.Fatal("Empty job was accepted")
	}

	task := sched.Task{}
	task.Command = []string{"echo", "foo"}
	job.Tasks = []sched.Task{task}
	_, err = q.Enqueue(job)
	if err == nil {
		t.Fatal("Task with no id accepted")
	}

	task = sched.Task{}
	task.Id = "foo"
	job.Tasks = []sched.Task{task}
	_, err = q.Enqueue(job)
	if err == nil {
		t.Fatal("Task with no Command accepted")
	}
}

func TestEnqueue(t *testing.T) {
	q, ch := memory.NewSimpleQueue()
	job := sched.Job{}
	task := sched.Task{}
	task.Id = "1"
	task.Command = []string{"echo", "foo"}
	task.SnapshotId = "snapshot-id"
	job.Tasks = []sched.Task{task}
	id, err := q.Enqueue(job)
	if err != nil {
		t.Fatalf("Error enqueueing %v", err)
	}
	outJob := <-ch
	if outJob.Id != id {
		t.Fatalf("Unexpected jobId %v (expected %v)", outJob.Id, id)
	}
	if len(outJob.Tasks) != 1 {
		t.Fatalf("Unexpected task length %v (expected 1)", len(outJob.Tasks))
	}
	outTask := outJob.Tasks[0]
	if outTask.Id != task.Id {
		t.Fatalf("Unequal task.Id %v %v", outTask.Id, task.Id)
	}
	if len(outTask.Command) != 2 || outTask.Command[0] != "echo" || outTask.Command[1] != "foo" {
		t.Fatalf("Unequal task.Command %v %v", outTask.Command, task.Command)
	}
	if outTask.SnapshotId != task.SnapshotId {
		t.Fatalf("Unequal task.SnapshotId %v %v", outTask.SnapshotId, task.SnapshotId)
	}
}
