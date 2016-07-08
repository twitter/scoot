package server_test

import (
	"github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/scootapi/server"
	"testing"
)

func TestRunBadJobFails(t *testing.T) {
	q, _ := memory.NewSimpleQueue()
	defer q.Close()
	handler := server.NewHandler(q)

	jobDef := scoot.NewJobDefinition()

	_, err := handler.RunJob(jobDef)
	if err == nil {
		t.Fatalf("Expected err enqueueing empty job")
	}
	if err != nil {
		_, ok := err.(*scoot.InvalidRequest)
		if !ok {
			t.Fatalf("Didn't get InvalidRequest %v", err)
		}
	}

	task := scoot.NewTask()
	task.Command = scoot.NewCommand()
	task.Command.Argv = []string{}
	task.SnapshotId = new(string)
	task.ID = new(string)
	*task.ID = "task1"
	jobDef = scoot.NewJobDefinition()
	jobDef.Tasks = append(jobDef.Tasks, task)

	_, err = handler.RunJob(jobDef)
	if err == nil {
		t.Fatalf("Expected err enqueing job with no command")
	}
	if err != nil {
		_, ok := err.(*scoot.InvalidRequest)
		if !ok {
			t.Fatalf("Didn't get InvalidRequest %v", err)
		}
	}
}

func TestRunSimpleJob(t *testing.T) {
	q, _ := memory.NewSimpleQueue()
	defer q.Close()
	handler := server.NewHandler(q)

	task := scoot.NewTask()
	task.Command = scoot.NewCommand()
	task.Command.Argv = []string{"true"}
	task.SnapshotId = new(string)
	task.ID = new(string)
	*task.ID = "task1"
	jobDef := scoot.NewJobDefinition()
	jobDef.Tasks = append(jobDef.Tasks, task)

	_, err := handler.RunJob(jobDef)
	if err != nil {
		t.Fatalf("Can't enqueue job: %v", err)
	}

}

func TestQueueFillsAndEmpties(t *testing.T) {
	q, itemCh := memory.NewSimpleQueue()
	defer q.Close()
	handler := server.NewHandler(q)

	task := scoot.NewTask()
	task.Command = scoot.NewCommand()
	task.Command.Argv = []string{"true"}
	task.SnapshotId = new(string)
	task.ID = new(string)
	*task.ID = "task1"
	jobDef := scoot.NewJobDefinition()
	jobDef.Tasks = append(jobDef.Tasks, task)

	_, err := handler.RunJob(jobDef)
	if err != nil {
		t.Fatalf("can't enqueue job: %v", err)
	}

	// Now retry, and queue should be full
	_, err = handler.RunJob(jobDef)
	if err == nil {
		t.Fatalf("expected queue to be full")
	}
	_, ok := err.(*scoot.CanNotScheduleNow)
	if !ok {
		t.Fatalf("expected queue to be full %v", err)
	}

	// Empty queue
	item := <-itemCh
	item.Dequeue()

	_, err = handler.RunJob(jobDef)
	if err != nil {
		t.Fatalf("can't enqueue after emptying: %v", err)
	}
}
