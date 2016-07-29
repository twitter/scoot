package server_test

import (
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/scootapi/server"
	"testing"
)

func CreateSagaCoordMock(t *testing.T) (saga.SagaCoordinator, *saga.MockSagaLog) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sagaCoord := saga.MakeSagaCoordinator(sagaLogMock)

	return sagaCoord, sagaLogMock
}

func TestRunBadJobFails(t *testing.T) {
	q := memory.NewSimpleQueue(1)
	defer q.Close()
	sc, _ := CreateSagaCoordMock(t)

	handler := server.NewHandler(q, sc)

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

	task := scoot.NewTaskDefinition()
	task.Command = scoot.NewCommand()
	task.Command.Argv = []string{}
	task.SnapshotId = new(string)
	jobDef = scoot.NewJobDefinition()
	jobDef.Tasks = map[string]*scoot.TaskDefinition{
		"task1": task,
	}

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
	q := memory.NewSimpleQueue(1)
	defer q.Close()
	sc, _ := CreateSagaCoordMock(t)

	handler := server.NewHandler(q, sc)

	task := scoot.NewTaskDefinition()
	task.Command = scoot.NewCommand()
	task.Command.Argv = []string{"true"}
	task.SnapshotId = new(string)
	jobDef := scoot.NewJobDefinition()
	jobDef.Tasks = map[string]*scoot.TaskDefinition{
		"task1": task,
	}

	_, err := handler.RunJob(jobDef)
	if err != nil {
		t.Fatalf("Can't enqueue job: %v", err)
	}
}

type errQueue struct{}

func (q *errQueue) Enqueue(job sched.JobDefinition) (string, error) {
	return "", fmt.Errorf("Not connected")
}

func (q *errQueue) Chan() chan queue.WorkItem { return nil }

func (q *errQueue) Close() error {
	return nil
}

func TestQueueError(t *testing.T) {
	q := &errQueue{}
	defer q.Close()
	sc, _ := CreateSagaCoordMock(t)

	handler := server.NewHandler(q, sc)

	task := scoot.NewTaskDefinition()
	task.Command = scoot.NewCommand()
	task.Command.Argv = []string{"true"}
	task.SnapshotId = new(string)
	jobDef := scoot.NewJobDefinition()
	jobDef.Tasks = map[string]*scoot.TaskDefinition{
		"task1": task,
	}

	_, err := handler.RunJob(jobDef)
	if err == nil {
		t.Fatalf("expected enqueue to fail")
	}

}

func TestQueueFillsAndEmpties(t *testing.T) {
	q := memory.NewSimpleQueue(1)
	defer q.Close()
	sc, _ := CreateSagaCoordMock(t)

	handler := server.NewHandler(q, sc)

	task := scoot.NewTaskDefinition()
	task.Command = scoot.NewCommand()
	task.Command.Argv = []string{"true"}
	task.SnapshotId = new(string)
	jobDef := scoot.NewJobDefinition()
	jobDef.Tasks = map[string]*scoot.TaskDefinition{
		"task1": task,
	}

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
	item := <-q.Chan()
	item.Dequeue()

	_, err = handler.RunJob(jobDef)
	if err != nil {
		t.Fatalf("can't enqueue after emptying: %v", err)
	}
}
