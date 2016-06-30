package memory_test

import (
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/queue/memory"
	"testing"
)

func TestBadDef(t *testing.T) {
	q, _ := memory.NewSimpleQueue(1)
	job := sched.JobDefinition{}
	_, err := q.Enqueue(job)
	if err == nil {
		t.Fatal("Empty job was accepted")
	}

	task := sched.TaskDefinition{}
	job.Tasks = map[string]sched.TaskDefinition{"task": task}
	_, err = q.Enqueue(job)
	if err == nil {
		t.Fatal("Task with no Command accepted")
	}
}

func TestEnqueue(t *testing.T) {
	q := memory.NewSimpleQueue(1)
	ch := q.Chan()

	job := sched.JobDefinition{}
	task := sched.TaskDefinition{}
	task.Command = sched.Command{Argv: []string{"echo", "foo"}}
	task.SnapshotId = "snapshot-id"
	job.Tasks = map[string]sched.TaskDefinition{"task": task}
	id, err := q.Enqueue(job)
	if err != nil {
		t.Fatalf("Error enqueueing %v", err)
	}
	out := <-ch
	outJob := out.Job()
	if outJob.Id != id {
		t.Fatalf("Unexpected jobId %v (expected %v)", outJob.Id, id)
	}
	if len(outJob.Def.Tasks) != 1 {
		t.Fatalf("Unexpected task length %v (expected 1)", len(outJob.Def.Tasks))
	}
	outTask, ok := outJob.Def.Tasks["task"]
	if !ok {
		t.Fatalf("No task \"task\" %v", outJob.Def.Tasks)
	}
	if len(outTask.Command.Argv) != 2 || outTask.Command.Argv[0] != "echo" || outTask.Command.Argv[1] != "foo" {
		t.Fatalf("Unequal task.Command %v %v", outTask.Command.Argv, task.Command.Argv)
	}
	if outTask.SnapshotId != task.SnapshotId {
		t.Fatalf("Unequal task.SnapshotId %v %v", outTask.SnapshotId, task.SnapshotId)
	}
	out.Dequeue()
}

func TestBackpressure(t *testing.T) {
	q := memory.NewSimpleQueue(1)
	job := sched.JobDefinition{}
	task := sched.TaskDefinition{}
	task.Command = sched.Command{Argv: []string{"echo", "foo"}}
	task.SnapshotId = "snapshot-id"
	job.Tasks = map[string]sched.TaskDefinition{"task": task}
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
