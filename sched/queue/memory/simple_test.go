package memory_test

import (
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/queue/memory"
	"testing"
)

func TestBadDef(t *testing.T) {
	q := memory.NewSimpleQueue(1)
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
	q := memory.NewSimpleQueue(1)
	ch := q.Chan()
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
	out := <-ch
	outJob := out.Job()
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
	out.Dequeue()
}

func TestBackpressure(t *testing.T) {
	q := memory.NewSimpleQueue(1)
	job := sched.Job{}
	task := sched.Task{}
	task.Id = "1"
	task.Command = []string{"echo", "foo"}
	task.SnapshotId = "snapshot-id"
	job.Tasks = []sched.Task{task}
	_, err := q.Enqueue(job)
	if err != nil {
		t.Fatalf("Error enqueueing %v", err)
	}
	_, err = q.Enqueue(job)
	if err == nil {
		t.Fatalf("No error enqueueing a second job")
	}
	switch err := err.(type) {
	case *queue.CanNotScheduleNow:
		// All good!
	default:
		t.Fatalf("Unexpected error when enqueueing a second job %v", err)
	}

	q.Close()
}
