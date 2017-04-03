package scheduler

import (
	"log"
	"time"

	"github.com/scootdev/scoot/cloud/cluster"
)

const noTask = ""
const maxLostDuration = time.Minute  // after which we remove a node from the cluster entirely
const maxFlakyDuration = time.Minute // after which we mark it not flaky and put it back in rotation.
var nilTime = time.Time{}

// clusterState maintains a cluster of nodes and information about what task is running on each node.
// nodeGroups is for node affinity where we want to remember which node last ran with what snapshot.
type clusterState struct {
	updateCh   chan []cluster.NodeUpdate
	nodes      map[cluster.NodeId]*nodeState // All healthy nodes and nodes temporarily marked flaky.
	lostNodes  map[cluster.NodeId]*nodeState // All lost nodes, disjoint from 'nodes' above.
	flakyNodes map[cluster.NodeId]*nodeState // All flaky nodes, included in 'nodes' above.
	nodeGroups map[string]*nodeGroup         //key is a snapshotId.
}

type nodeGroup struct {
	idle map[cluster.NodeId]*nodeState
	busy map[cluster.NodeId]*nodeState
}

func newNodeGroup() *nodeGroup {
	return &nodeGroup{idle: map[cluster.NodeId]*nodeState{}, busy: map[cluster.NodeId]*nodeState{}}
}

// The State of A Node in the Cluster
type nodeState struct {
	node        cluster.Node
	runningTask string
	snapshotId  string
	timeLost    time.Time // Time when node was marked lost, if set.
	timeFlaky   time.Time // Time when node was marked flaky, if set.
}

func (ns *nodeState) Lost() bool {
	return ns.timeLost != nilTime
}

func (ns *nodeState) Flaky() bool {
	return ns.timeFlaky != nilTime
}

// Initializes a Node State for the specified Node
func newNodeState(node cluster.Node) *nodeState {
	return &nodeState{
		node:        node,
		runningTask: noTask,
		snapshotId:  "",
	}
}

// Creates a New State Distributor with the initial nodes, and which updates
// nodes added or removed based on the supplied channel.
func newClusterState(initial []cluster.Node, updateCh chan []cluster.NodeUpdate) *clusterState {
	nodes := make(map[cluster.NodeId]*nodeState)
	nodeGroups := map[string]*nodeGroup{"": newNodeGroup()}
	for _, n := range initial {
		nodes[n.Id()] = newNodeState(n)
		nodeGroups[""].idle[n.Id()] = nodes[n.Id()]
	}

	return &clusterState{
		updateCh:   updateCh,
		nodes:      nodes,
		lostNodes:  map[cluster.NodeId]*nodeState{},
		flakyNodes: map[cluster.NodeId]*nodeState{},
		nodeGroups: nodeGroups,
	}
}

// Update ClusterState to reflect that a task has been scheduled on a particular node
// SnapshotId should be the value from the task definition associated with the given taskId.
// TODO: taskId is not unique (and isn't currently required to be), but a jobId arg would fix that.
func (c *clusterState) taskScheduled(nodeId cluster.NodeId, taskId string, snapshotId string) {
	ns := c.nodes[nodeId]

	delete(c.nodeGroups[ns.snapshotId].idle, nodeId)
	if _, ok := c.nodeGroups[snapshotId]; !ok {
		c.nodeGroups[snapshotId] = newNodeGroup()
	}
	c.nodeGroups[snapshotId].busy[nodeId] = ns

	ns.snapshotId = snapshotId
	ns.runningTask = taskId
}

// Update ClusterState to reflect that a task has finished running on
// a particular node, whether successfully or unsuccessfully
func (c *clusterState) taskCompleted(nodeId cluster.NodeId, taskId string, flaky bool) {
	// this node may have been removed from the cluster in the last update
	if ns, ok := c.nodes[nodeId]; ok {
		if flaky {
			c.flakyNodes[nodeId] = ns
			ns.timeFlaky = time.Now()
		}
		ns.runningTask = noTask
		delete(c.nodeGroups[ns.snapshotId].busy, nodeId)
		c.nodeGroups[ns.snapshotId].idle[nodeId] = ns
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
	// Apply updates
	for _, update := range updates {
		switch update.UpdateType {
		case cluster.NodeAdded:
			if ns, ok := c.lostNodes[update.Id]; ok {
				// This node was lost earlier, we can recover it now.
				ns.timeLost = nilTime
				c.nodes[update.Id] = ns
				delete(c.lostNodes, update.Id)
				log.Printf("Found lost node %v (%v), now have %d healthy nodes, %d lost nodes",
					update.Id, ns, len(c.nodes), len(c.lostNodes))
			} else if ns, ok := c.nodes[update.Id]; !ok {
				// This is a new unrecognized node, add it to the cluster.
				c.nodes[update.Id] = newNodeState(update.Node)
				c.nodeGroups[""].idle[update.Id] = c.nodes[update.Id]
				log.Printf("Added new node: %v (%+v), now have %d nodes\n", update.Id, update.Node, len(c.nodes))
			} else {
				// This node is already present, log this spurious add.
				log.Printf("Node already added!! %v (%v)", update.Id, ns)
			}
		case cluster.NodeRemoved:
			//log.Printf("Removed nodeId: %s (%v), now have %d nodes\n", string(update.Id), c.nodes[update.Id], len(c.nodes))
			if ns, ok := c.lostNodes[update.Id]; ok {
				// Node already lost, log this spurious remove.
				log.Printf("Node already marked lost: %v (%v)", update.Id, ns)
			} else if ns, ok := c.nodes[update.Id]; ok {
				// This was a healthy node, mark it as lost now.
				ns.timeLost = time.Now()
				c.lostNodes[update.Id] = ns
				delete(c.nodes, update.Id)
				log.Printf("Removing node by marking as lost: %v (%v), now have %d nodes\n", update.Id, ns, len(c.nodes))
			} else {
				// We don't know about this node, log spurious remove.
				log.Printf("Cannot remove unknown node: %v", update.Id)
			}

		}
	}

	// Clean up lost nodes that haven't recovered in time, and put flaky nodes back in rotation.
	now := time.Now()
	for _, ns := range c.lostNodes {
		if now.Sub(ns.timeLost) > maxLostDuration {
			delete(c.lostNodes, ns.node.Id())
			delete(c.nodeGroups[ns.snapshotId].idle, ns.node.Id())
			delete(c.nodeGroups[ns.snapshotId].busy, ns.node.Id())
		}
	}
	for _, ns := range c.flakyNodes {
		if now.Sub(ns.timeFlaky) > maxFlakyDuration {
			delete(c.flakyNodes, ns.node.Id())
			ns.timeFlaky = nilTime
		}
	}
}
