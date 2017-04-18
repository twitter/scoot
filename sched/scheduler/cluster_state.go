package scheduler

import (
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/cloud/cluster"
)

const noTask = ""
const defaultMaxLostDuration = time.Minute
const defaultMaxFlakyDuration = time.Minute

var nilTime = time.Time{}

// Cluster will use this function to determine if newly added nodes are ready to be used.
type ReadyFn func(cluster.Node) (ready bool, backoffDuration time.Duration)

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
	timeLost    time.Time        // Time when node was marked lost, if set (lost and flaky are mutually exclusive).
	timeFlaky   time.Time        // Time when node was marked flaky, if set (lost and flaky are mutually exclusive).
	readyCh     chan interface{} // We create goroutines for each new node which will close this channel once the node is ready.
	removedCh   chan interface{} // We send nil when a node has been removed and we want the above goroutine to exit.
}

// This node was either reported lost by a NodeUpdate and we keep it around for a bit in case it revives,
// or it experienced connection related errors so we sideline it for a little while.
func (ns *nodeState) suspended() bool {
	return ns.readyCh != nil || ns.timeLost != nilTime || ns.timeFlaky != nilTime
}

// This node is ready if the readyCh has been closed, either upon creation or in the startReadyLoop() goroutine.
func (ns *nodeState) ready() bool {
	ready := (ns.readyCh == nil)
	if !ready {
		select {
		case <-ns.readyCh:
			ready = true
		default:
		}
	}
	return ready
}

// Stars a goroutine loop checking node readiness, waiting 'backoff' time between checks, and exiting if node is fully removed.
func (ns *nodeState) startReadyLoop(rfn ReadyFn) {
	go func() {
		done := false
		for !done {
			if ready, backoff := rfn(ns.node); ready {
				close(ns.readyCh)
				done = true
			} else {
				select {
				case <-ns.removedCh:
					done = true
				case <-time.After(backoff):
					break
				}
			}
		}
	}()
}

// Initializes a Node State for the specified Node
func newNodeState(node cluster.Node) *nodeState {
	return &nodeState{
		node:        node,
		runningTask: noTask,
		snapshotId:  "",
		timeLost:    nilTime,
		timeFlaky:   nilTime,
		readyCh:     make(chan interface{}),
		removedCh:   make(chan interface{}),
	}
}

// Creates a New State Distributor with the initial nodes, and which updates
// nodes added or removed based on the supplied channel. ReadyFn is optional.
// New cluster is returned along with a doneCh which the caller can close to exit our goroutine.
func newClusterState(initial []cluster.Node, updateCh chan []cluster.NodeUpdate, rfn ReadyFn) *clusterState {
	nodes := make(map[cluster.NodeId]*nodeState)
	nodeGroups := map[string]*nodeGroup{"": newNodeGroup()}
	suspendedNodes := map[cluster.NodeId]*nodeState{}
	for _, n := range initial {
		nodes[n.Id()] = newNodeState(n)
		nodeGroups[""].idle[n.Id()] = nodes[n.Id()]
		if rfn == nil {
			nodes[n.Id()].readyCh = nil
		} else {
			nodes[n.Id()].startReadyLoop(rfn)
		}
	}
	if rfn != nil {
		nodes, suspendedNodes = suspendedNodes, nodes
	}
	return &clusterState{
		updateCh:         updateCh,
		nodes:            nodes,
		suspendedNodes:   suspendedNodes,
		nodeGroups:       nodeGroups,
		maxLostDuration:  defaultMaxLostDuration,
		maxFlakyDuration: defaultMaxFlakyDuration,
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
		c.update(nil)
	}
}

// Processes nodes being added and removed from the cluster & updates the distributor state accordingly.
// Note, we don't expect there to be many updates after startup if the cluster is relatively stable.
//TODO(jschiller) this assumes that new nodes never have the same id as previous ones but we shouldn't rely on that.
func (c *clusterState) update(updates []cluster.NodeUpdate) {
	// Apply updates
	for _, update := range updates {
		var newNode *nodeState
		switch update.UpdateType {

		case cluster.NodeAdded:
			if ns, ok := c.suspendedNodes[update.Id]; ok {
				if !ns.ready() {
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
				if c.readyFn == nil {
					// We're not checking readiness, skip suspended state and add this as a healthy node.
					newNode = newNodeState(update.Node)
					c.nodes[update.Id] = newNode
					log.Infof("Added new node: %v (%#v), now have %d healthy nodes (%d suspended)",
						update.Id, update.Node, len(c.nodes), len(c.suspendedNodes))
				} else {
					// Add this to the suspended nodes and start a goroutine to check for readiness.
					newNode = newNodeState(update.Node)
					c.suspendedNodes[update.Id] = newNode
					log.Infof("Added new suspended node: %v (%#v), now have %d healthy nodes (%d suspended)",
						update.Id, update.Node, len(c.nodes), len(c.suspendedNodes))
					newNode.startReadyLoop(c.readyFn)
				}
				c.nodeGroups[""].idle[update.Id] = newNode

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
		if ns.ready() && ns.timeLost == nilTime {
			// This node is initialized, remove it from suspended nodes and add it to the healthy node pool.
			log.Infof("Node now ready, adding to rotation: %v (%#v), now have %d healthy (%d suspended)",
				ns.node.Id(), ns, len(c.nodes), len(c.suspendedNodes))
			ns.readyCh = nil
			c.nodes[ns.node.Id()] = ns
			delete(c.suspendedNodes, ns.node.Id())
		} else if ns.timeLost != nilTime && now.Sub(ns.timeLost) > c.maxLostDuration {
			// This node has been missing too long, delete all references to it.
			// Try to notify this node's goroutine about removal so it can stop checking readiness if necessary.
			log.Infof("Deleting lost node: %v (%#v), now have %d healthy (%d suspended)",
				ns.node.Id(), ns, len(c.nodes), len(c.suspendedNodes)-1)
			delete(c.suspendedNodes, ns.node.Id())
			delete(c.nodeGroups[ns.snapshotId].idle, ns.node.Id())
			delete(c.nodeGroups[ns.snapshotId].busy, ns.node.Id())
			select {
			case ns.removedCh <- nil:
			default:
			}
		} else if ns.timeFlaky != nilTime && now.Sub(ns.timeFlaky) > c.maxFlakyDuration {
			// This flaky node has been suspended long enough, try adding it back to the healthy node pool.
			log.Infof("Reinstating flaky node: %v (%#v), now have %d healthy (%d suspended)",
				ns.node.Id(), ns, len(c.nodes), len(c.suspendedNodes))
			delete(c.suspendedNodes, ns.node.Id())
			c.nodes[ns.node.Id()] = ns
			ns.timeFlaky = nilTime
		}
	}
}
