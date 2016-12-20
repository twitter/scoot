package scheduler

import (
	"github.com/scootdev/scoot/cloud/cluster"
)

const noTask = ""

// clusterState maintains a cluster of nodes
// and information about what task is running on each node
type clusterState struct {
	updateCh chan []cluster.NodeUpdate
	nodes    map[cluster.NodeId]*nodeState
}

// The State of A Node in the Cluster
type nodeState struct {
	node        cluster.Node
	runningTask string
}

// Initializes a Node State for the specified Node
func newNodeState(node cluster.Node) *nodeState {
	return &nodeState{
		node:        node,
		runningTask: noTask,
	}
}

// Creates a New State Distributor with the initial nodes, and which updates
// nodes added or removed based on the supplied channel.
func newClusterState(initial []cluster.Node, updateCh chan []cluster.NodeUpdate) *clusterState {

	nodes := make(map[cluster.NodeId]*nodeState)
	for _, n := range initial {
		nodes[n.Id()] = newNodeState(n)
	}

	cs := &clusterState{
		updateCh: updateCh,
		nodes:    nodes,
	}

	return cs
}

// Update ClusterState to reflect that a task has been scheduled on a
// particular node
func (c *clusterState) taskScheduled(nodeId cluster.NodeId, taskId string) {
	ns := c.nodes[nodeId]
	ns.runningTask = taskId
}

// Update ClusterState to reflect that a task has finished running on
// a particular node, whether successfully or unsuccessfully
func (c *clusterState) taskCompleted(nodeId cluster.NodeId, taskId string) {
	// this node may have been removed from the cluster in the last update
	ns, ok := c.nodes[nodeId]
	if ok {
		ns.runningTask = noTask
	}
}

func (c *clusterState) getNodeState(nodeId cluster.NodeId) (*nodeState, bool) {
	ns, ok := c.nodes[nodeId]
	return ns, ok
}

// upate cluster state to reflect added and removed nodes
func (c *clusterState) updateCluster() {
	select {
	case updates, ok := <-c.updateCh:
		if !ok {
			c.updateCh = nil
		}
		c.update(updates)
	default:
	}
}

// Processes nodes being added and removed from the cluster &
// updates the distributor state accordingly
func (c *clusterState) update(updates []cluster.NodeUpdate) {
	for _, update := range updates {
		switch update.UpdateType {
		case cluster.NodeAdded:
			// add the node if it doesn't already exist
			if _, ok := c.nodes[update.Node.Id()]; !ok {
				c.nodes[update.Node.Id()] = newNodeState(update.Node)
			}
		case cluster.NodeRemoved:
			delete(c.nodes, update.Id)
		}
	}

}
