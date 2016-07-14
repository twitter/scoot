package fake

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/worker"
	"math/rand"
	"time"
)

type noopWorkerController struct{}

func NewNoopWorker() worker.WorkerController {
	return &noopWorkerController{}
}

func (c *noopWorkerController) RunAndWait(task sched.TaskDefinition) error {
	return nil
}

func MakeNoopWorker(node cluster.Node) worker.WorkerController {
	return &noopWorkerController{}
}

type waitingWorkerController struct {
	delegate worker.WorkerController
}

func MakeWaitingNoopWorker(node cluster.Node) worker.WorkerController {
	return &waitingWorkerController{
		&noopWorkerController{},
	}
}

func (c *waitingWorkerController) RunAndWait(task sched.TaskDefinition) error {
	result := c.delegate.RunAndWait(task)

	//delay message to mimic network call
	delayMS := time.Duration(rand.Intn(500)) * time.Millisecond
	time.Sleep(delayMS)
	return result
}

type panicWorkerController struct{}

func NewPanicWorker() worker.WorkerController {
	return &panicWorkerController{}
}

func (c *panicWorkerController) RunAndWait(task sched.TaskDefinition) error {
	panic("panicWorkerController panics")
}
