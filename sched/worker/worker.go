package worker

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/sched"
)

// Create a WorkerController that talks to node
type WorkerFactory func(node cluster.Node) WorkerController

// WorkerController lets the Scheduler control a worker.
type WorkerController interface {
	// TODO(dbentley): include more info on positive results
	RunAndWait(task sched.TaskDefinition) error
}

type workerNode struct {
	worker WorkerController
	node   cluster.Node
}

func (n *workerNode) Worker() WorkerController { return n.worker }
func (n *workerNode) Id() cluster.NodeId       { return n.node.Id() }

type Command struct{}

type RunId string

type RunStatus struct{}

type WorkerStatus struct{}

// Worker lets the WorkerController talk to a Worker instance.
// This is an interface so that we can have separate implementations:
// *rpc/thriftWorker will send RPCs to workers on other machines
// *fake/localWorker will use a *scoot/runner/simple/simpleRunner to fake running
type Worker interface {
	// Runs the command.
	// Blocks until the worker has accepted the command and either started running it or returned an error
	Run(cmd Command) (RunId, error)

	Query(run RunId) (RunStatus, error)

	WorkerStatus() (WorkerStatus, error)

	// Wait for run to be done
	Wait(run RunId) (RunStatus, error)
}
