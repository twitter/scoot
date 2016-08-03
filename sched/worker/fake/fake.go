package fake

// import (
// 	"math/rand"
// 	"time"

// 	"github.com/scootdev/scoot/cloud/cluster"
// 	"github.com/scootdev/scoot/sched"
// 	"github.com/scootdev/scoot/sched/worker"
// )

// type noopWorker struct{}

// func NewNoopWorker() worker.Worker {
// 	return &noopWorker{}
// }

// func (c *noopWorker) RunAndWait(task sched.TaskDefinition) error {
// 	return nil
// }

// func MakeNoopWorker(node cluster.Node) worker.Worker {
// 	return &noopWorker{}
// }

// type waitingWorker struct {
// 	delegate worker.Worker
// }

// func MakeWaitingNoopWorker(node cluster.Node) worker.Worker {
// 	return &waitingWorker{
// 		&noopWorker{},
// 	}
// }

// func (c *waitingWorker) RunAndWait(task sched.TaskDefinition) error {
// 	result := c.delegate.RunAndWait(task)

// 	//delay message to mimic network call
// 	delayMS := time.Duration(rand.Intn(500)) * time.Millisecond
// 	time.Sleep(delayMS)
// 	return result
// }

// type panicWorker struct{}

// func NewPanicWorker() worker.Worker {
// 	return &panicWorker{}
// }

// func (c *panicWorker) RunAndWait(task sched.TaskDefinition) error {
// 	panic("panicWorker panics")
// }
