package worker

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/sched"
)

// Create a Worker controller that talks to node
type WorkerFactory func(node cluster.Node) Worker

// Worker controller gives the Scheduler a generic way to complete work.
type Worker interface {
	// TODO(dbentley): include more info on positive results
	RunAndWait(task sched.TaskDefinition) error
}

type workerNode struct {
	worker Worker
	node   cluster.Node
}

func (n *workerNode) Worker() Worker     { return n.worker }
func (n *workerNode) Id() cluster.NodeId { return n.node.Id() }
