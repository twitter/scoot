package scheduler

import (
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/cloud/cluster"
)

const noTask = ""
const defaultMaxLostDuration = time.Minute
const defaultMaxFlakyDuration = time.Minute
const defaultReadyFnInterval = 5 * time.Second

var nilTime = time.Time{}

type ReadyFn func(cluster.Node) bool

// clusterState maintains a cluster of nodes and information about what task is running on each node.
// nodeGroups is for node affinity where we want to remember which node last ran with what snapshot.
// TODO(jschiller): we may prefer to assert that updateCh never blips on a node so we can remove lost node concept.
type clusterState struct {
	updateCh         chan []cluster.NodeUpdate
	nodes            map[cluster.NodeId]*nodeState // All healthy nodes.
	suspendedNodes   map[cluster.NodeId]*nodeState // All new, lost, or flaky nodes, disjoint from 'nodes'.
	nodeGroups       map[string]*nodeGroup         // key is a snapshotId.
	maxLostDuration  time.Duration                 // after which we remove a node from the cluster entirely
	maxFlakyDuration time.Duration                 // after which we mark it not flaky and put it back in rotation.
	readyFnInterval  time.Duration                 // how often we should call readyFn to validate node.
	readyFn          ReadyFn                       // If provided, new nodes will be suspended until this returns true.
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
	timeInit    time.Time // Time when new node was marked init'ing. Unset once init completes.
	timeLost    time.Time // Time when node was marked lost, if set (lost and flaky are mutually exclusive).
	timeFlaky   time.Time // Time when node was marked flaky, if set (lost and flaky are mutually exclusive).
}

// This node was either reported lost by a NodeUpdate and we keep it around for a bit in case it revives,
// or it experienced connection related errors so we sideline it for a little while.
func (ns *nodeState) suspended() bool {
	return ns.timeInit != nilTime || ns.timeLost != nilTime || ns.timeFlaky != nilTime
}

// Initializes a Node State for the specified Node
func newNodeState(node cluster.Node) *nodeState {
	return &nodeState{
		node:        node,
		runningTask: noTask,
		snapshotId:  "",
		timeInit:    nilTime,
		timeLost:    nilTime,
		timeFlaky:   nilTime,
	}
}

// Creates a New State Distributor with the initial nodes, and which updates
// nodes added or removed based on the supplied channel. ReadyFn is optional.
func newClusterState(initial []cluster.Node, updateCh chan []cluster.NodeUpdate, rfn ReadyFn) *clusterState {
	nodes := make(map[cluster.NodeId]*nodeState)
	nodeGroups := map[string]*nodeGroup{"": newNodeGroup()}
	for _, n := range initial {
		nodes[n.Id()] = newNodeState(n)
		nodeGroups[""].idle[n.Id()] = nodes[n.Id()]
	}

	return &clusterState{
		updateCh:         updateCh,
		nodes:            nodes,
		suspendedNodes:   map[cluster.NodeId]*nodeState{},
		nodeGroups:       nodeGroups,
		maxLostDuration:  defaultMaxLostDuration,
		maxFlakyDuration: defaultMaxFlakyDuration,
		readyFnInterval:  defaultReadyFnInterval,
		readyFn:          rfn,
	}
}

// Update ClusterState to reflect that a task has been scheduled on a particular node
// SnapshotId should be the value from the task definition associated with the given taskId.
// NOTE: taskId is not unique (and isn't currently required to be), but a jobId arg would fix that.
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
	var ns *nodeState
	var ok bool
	if ns, ok = c.nodes[nodeId]; !ok {
		// This node was removed from the cluster already, check if it was moved to suspendedNodes.
		ns, ok = c.suspendedNodes[nodeId]
	}
	if ok {
		if flaky && !ns.suspended() {
			delete(c.nodes, nodeId)
			c.suspendedNodes[nodeId] = ns
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

// Processes nodes being added and removed from the cluster & updates the distributor state accordingly.
// Note, we don't expect there to be many updates after startup if the cluster is relatively stable.
//TODO(jschiller) this assumes that new nodes never have the same id as previous ones but we can't rely on that.
func (c *clusterState) update(updates []cluster.NodeUpdate) {
	// Apply updates
	for _, update := range updates {
		switch update.UpdateType {
		case cluster.NodeAdded:
			if ns, ok := c.suspendedNodes[update.Id]; ok {
				if ns.timeInit != nilTime {
					// Adding a node that's already suspended as non-ready, leave it in that state until ready.
					log.Infof("Suspended node re-added but still awaiting readiness check %v (%#v)", update.Id, ns)
				} else {
					// This node was suspended earlier, we can recover it now.
					ns.timeLost = nilTime
					ns.timeFlaky = nilTime
					c.nodes[update.Id] = ns
					delete(c.suspendedNodes, update.Id)
					log.Infof("Recovered suspended node %v (%#v), now have %d healthy nodes (%d suspended)",
						update.Id, ns, len(c.nodes), len(c.suspendedNodes))
				}

			} else if ns, ok := c.nodes[update.Id]; !ok {
				// This is a new unrecognized node, add it to the cluster, possibly in a suspended state.
				if c.readyFn == nil || c.readyFn(ns.node) {
					// We're either not checking readiness or it is ready, skip suspended state and add this as a healthy node.
					c.nodes[update.Id] = newNodeState(update.Node)
					log.Infof("Added new node: %v (%#v), now have %d healthy nodes (%d suspended)",
						update.Id, update.Node, len(c.nodes), len(c.suspendedNodes))
				} else {
					// Add this to the suspended nodes to be checked on every update().
					c.suspendedNodes[update.Id] = newNodeState(update.Node)
					c.suspendedNodes[update.Id].timeInit = time.Now()
					log.Infof("Added new suspended node: %v (%#v), now have %d healthy nodes (%d suspended)",
						update.Id, update.Node, len(c.nodes), len(c.suspendedNodes))
				}
				c.nodeGroups[""].idle[update.Id] = c.nodes[update.Id]

			} else {
				// This node is already present, log this spurious add.
				log.Infof("Node already added!! %v (%#v)", update.Id, ns)
			}
		case cluster.NodeRemoved:
			if ns, ok := c.suspendedNodes[update.Id]; ok {
				// Node already suspended, make sure it's now marked as lost and not flaky (keep readiness status intact).
				log.Infof("Already suspended node marked as removed: %v (was %#v)", update.Id, ns)
				ns.timeLost = time.Now()
				ns.timeFlaky = nilTime

			} else if ns, ok := c.nodes[update.Id]; ok {
				// This was a healthy node, mark it as lost now.
				ns.timeLost = time.Now()
				c.suspendedNodes[update.Id] = ns
				delete(c.nodes, update.Id)
				log.Infof("Removing node by marking as lost: %v (%#v), now have %d nodes (%d suspended)",
					update.Id, ns, len(c.nodes), len(c.suspendedNodes))

			} else {
				// We don't know about this node, log spurious remove.
				log.Infof("Cannot remove unknown node: %v", update.Id)
			}
		}
	}

	// Clean up lost nodes that haven't recovered in time, add flaky nodes back into rotation after some time,
	// and check if newly added non-ready nodes are ready to be put into rotation.
	now := time.Now()
	for _, ns := range c.suspendedNodes {
		if ns.timeLost != nilTime && now.Sub(ns.timeLost) > c.maxLostDuration {
			log.Infof("Deleting lost node: %v (%#v), now have %d healthy (%d suspended)",
				ns.node.Id(), ns, len(c.nodes), len(c.suspendedNodes)-1)
			delete(c.suspendedNodes, ns.node.Id())
			delete(c.nodeGroups[ns.snapshotId].idle, ns.node.Id())
			delete(c.nodeGroups[ns.snapshotId].busy, ns.node.Id())

		} else if ns.timeFlaky != nilTime && now.Sub(ns.timeFlaky) > c.maxFlakyDuration {
			log.Infof("Reinstating flaky node: %v (%#v), now have %d healthy (%d suspended)",
				ns.node.Id(), ns, len(c.nodes), len(c.suspendedNodes))
			delete(c.suspendedNodes, ns.node.Id())
			c.nodes[ns.node.Id()] = ns
			ns.timeFlaky = nilTime

		} else if ns.timeInit != nilTime && now.Sub(ns.timeInit) > c.readyFnInterval {
			if c.readyFn(ns.node) {
				log.Infof("Node now ready, adding to rotation: %v (%#v), now have %d healthy (%d suspended)",
					ns.node.Id(), ns, len(c.nodes), len(c.suspendedNodes))
				c.nodes[ns.node.Id()] = ns
				ns.timeInit = nilTime
			} else {
				ns.timeInit = now
			}
		}
	}
}
