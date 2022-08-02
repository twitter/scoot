package server

import (
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"
	log "github.com/sirupsen/logrus"

	cc "github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/common/stats"
)

const noJob = ""
const noTask = ""
const defaultMaxLostDuration = time.Minute
const defaultMaxFlakyDuration = 15 * time.Minute

var nilTime = time.Time{}

// Cluster will use this function to determine if newly added nodes are ready to be used and also to
// determine if the node became unhealthy after running some task and is not ready to be used.
type ReadyFn func(cc.Node) (ready bool, backoffDuration time.Duration)

// clusterState maintains a cluster of nodes and information about what task is running on each node.
// nodeGroups is for node affinity where we want to remember which node last ran with what snapshot.
// NOTE: a node can be both running in scheduler and suspended here (distributed system eventual consistency...)
type clusterState struct {
	nodesUpdatesCh   chan []cc.NodeUpdate
	nodes            map[cc.NodeId]*nodeState // All healthy nodes.
	suspendedNodes   map[cc.NodeId]*nodeState // All new, lost, or flaky nodes, disjoint from 'nodes'.
	offlinedNodes    map[cc.NodeId]*nodeState // All User initiated offline nodes. Disjoint from 'nodes' & 'suspendedNodes'
	nodeGroups       map[string]*nodeGroup    // key is a snapshotId.
	maxLostDuration  time.Duration            // after which we remove a node from the cluster entirely
	maxFlakyDuration time.Duration            // after which we mark it not flaky and put it back in rotation.
	readyFn          ReadyFn                  // If provided, new or unhealthy nodes will be suspended until this returns true.
	numRunning       int                      // Number of running nodes. running + free + suspended ~= allNodes (may lag)
	stats            stats.StatsReceiver      // for collecting stats about node availability
	nopUpdateCnt     int
}

func (c *clusterState) isOfflined(ns *nodeState) bool {
	if _, ok := c.offlinedNodes[ns.node.Id()]; ok {
		return true
	}
	return false
}

type nodeGroup struct {
	idle map[cc.NodeId]*nodeState
	busy map[cc.NodeId]*nodeState
}

func newNodeGroup() *nodeGroup {
	return &nodeGroup{idle: map[cc.NodeId]*nodeState{}, busy: map[cc.NodeId]*nodeState{}}
}

// The State of A Node in the Cluster
type nodeState struct {
	node        cc.Node
	runningJob  string
	runningTask string
	snapshotId  string
	timeLost    time.Time        // Time when node was marked lost, if set (lost and flaky are mutually exclusive).
	timeFlaky   time.Time        // Time when node was marked flaky, if set (lost and flaky are mutually exclusive).
	readyCh     chan interface{} // We create goroutines for each new node which will close this channel once the node is ready.
	removedCh   chan interface{} // We send nil when a node has been removed and we want the above goroutine to exit.
}

func (n *nodeState) String() string {
	return fmt.Sprintf("{node:%s, jobId:%s, taskId:%s, snapshotId:%s, timeLost:%v, timeFlaky:%v, ready:%t}",
		spew.Sdump(n.node), n.runningJob, n.runningTask, n.snapshotId, n.timeLost, n.timeFlaky, (n.readyCh == nil))
}

// This node was either reported lost by a NodeUpdate and we keep it around for a bit in case it revives,
// or it experienced connection related errors so we sideline it for a little while.
func (ns *nodeState) suspended() bool {
	return ns.readyCh != nil || ns.timeLost != nilTime || ns.timeFlaky != nilTime
}

// This node is ready if the readyCh has been closed, either upon creation or in the startReadyLoop() goroutine.
// Note, nodes can be ready and healthy but still suspended due to update() lagging behind async startReadyLoop().
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

// Starts a goroutine loop checking node readiness, waiting 'backoff' time between checks, and exiting if node is fully removed.
func (ns *nodeState) startReadyLoop(rfn ReadyFn) {
	ns.readyCh = make(chan interface{})
	go func() {
		done := false
		for !done {
			if ready, backoff := rfn(ns.node); ready {
				close(ns.readyCh)
				done = true
			} else if backoff == 0 {
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
func newNodeState(node cc.Node) *nodeState {
	return &nodeState{
		node:        node,
		runningJob:  noJob,
		runningTask: noTask,
		snapshotId:  "",
		timeLost:    nilTime,
		timeFlaky:   nilTime,
		readyCh:     nil,
		removedCh:   make(chan interface{}),
	}
}

// Create a ClusterState which gets node updates from the buffered nodes updates channel.
// We use a buffered nodes updates channel to prevent cluster state from blocking the object that
// is recognizing the node updates (typically cluster).
// New Nodes are considered suspended till the optional ReadyFn reports the node is ready.
// ReadyFn is also used to communicate if the node becomes unhealthy after a task completion
// A ClusterState is returned.
func newClusterState(nodesUpdatesCh chan []cc.NodeUpdate, rfn ReadyFn, stats stats.StatsReceiver) *clusterState {
	cs := &clusterState{
		nodesUpdatesCh:   nodesUpdatesCh,
		nodes:            make(map[cc.NodeId]*nodeState),
		suspendedNodes:   map[cc.NodeId]*nodeState{},
		offlinedNodes:    make(map[cc.NodeId]*nodeState),
		nodeGroups:       map[string]*nodeGroup{"": newNodeGroup()},
		maxLostDuration:  defaultMaxLostDuration,
		maxFlakyDuration: defaultMaxFlakyDuration,
		readyFn:          rfn,
		stats:            stats,
	}
	cs.updateCluster()
	return cs
}

// return the number of free nodes that are not in a suspended state and not running tasks.
// Note: we assume numFree() is called from methods that have already created a (sync) lock
func (c *clusterState) numFree() int {
	// This can go negative due to lost nodes, set lower bound at zero.
	return max(0, len(c.nodes)-c.numRunning)
}

// update ClusterState to reflect that a task has been scheduled on a particular node
// SnapshotId should be the value from the task definition associated with the given taskId.
func (c *clusterState) taskScheduled(nodeId cc.NodeId, jobId, taskId, snapshotId string) {
	ns := c.nodes[nodeId]

	delete(c.nodeGroups[ns.snapshotId].idle, nodeId)
	empty := len(c.nodeGroups[ns.snapshotId].idle) == 0 && len(c.nodeGroups[ns.snapshotId].busy) == 0
	if ns.snapshotId != "" && empty {
		delete(c.nodeGroups, ns.snapshotId)
	}

	if _, ok := c.nodeGroups[snapshotId]; !ok {
		c.nodeGroups[snapshotId] = newNodeGroup()
	}
	c.nodeGroups[snapshotId].busy[nodeId] = ns

	ns.runningJob = jobId
	ns.runningTask = taskId
	ns.snapshotId = snapshotId
	c.numRunning++
}

// update ClusterState to reflect that a task has finished running on
// a particular node, whether successfully or unsuccessfully.
// If the node isn't found then the node was already suspended and deleted, just decrement numRunning.
func (c *clusterState) taskCompleted(nodeId cc.NodeId, flaky bool) {
	var ns *nodeState
	var ok bool
	if ns, ok = c.nodes[nodeId]; !ok {
		// This node was removed from the cluster already, check if it was moved to suspendedNodes.
		ns, ok = c.suspendedNodes[nodeId]
	}
	if ok {
		// check if node is ready after the last run, unhealthy nodes will not be ready
		// mark them as lost because we do not yet have a way to recover unhealthy nodes
		if c.readyFn != nil {
			if ready, _ := c.readyFn(ns.node); !ready && !ns.suspended() {
				delete(c.nodes, nodeId)
				c.suspendedNodes[nodeId] = ns
				ns.timeLost = time.Now()
			}
		}
		if flaky && !ns.suspended() {
			delete(c.nodes, nodeId)
			c.suspendedNodes[nodeId] = ns
			ns.timeFlaky = time.Now()
		}
		ns.runningJob = noJob
		ns.runningTask = noTask
		delete(c.nodeGroups[ns.snapshotId].busy, nodeId)
		c.nodeGroups[ns.snapshotId].idle[nodeId] = ns
	} else {
		log.Infof("TaskCompleted specified an unknown node: %v (flaky=%t) (likely reaped already)", nodeId, flaky)
	}
	c.numRunning--
}

func (c *clusterState) getNodeState(nodeId cc.NodeId) (*nodeState, bool) {
	ns, ok := c.nodes[nodeId]
	return ns, ok
}

// update cluster state to reflect added and removed nodes
// it will process at most DefaultClusterSize sets of updates with each call
func (c *clusterState) updateCluster() {
	defer c.stats.Latency(stats.SchedUpdateClusterLatency_ms).Time().Stop()
	allUpdates := []cc.NodeUpdate{}
LOOP:
	for i := 0; i < common.DefaultClusterChanSize; i++ {
		select {
		case updates := <-c.nodesUpdatesCh:
			allUpdates = append(allUpdates, updates...)
		default:
			break LOOP
		}
	}
	c.update(allUpdates)
}

// Processes nodes being added and removed from the cluster & updates the distributor state accordingly.
// Note, we don't expect there to be many updates after startup if the cluster is relatively stable.
// TODO(jschiller) this assumes that new nodes never have the same id as previous ones but we shouldn't rely on that.
func (c *clusterState) update(updates []cc.NodeUpdate) {
	// Apply updates
	adds := 0
	removals := 0
	for _, update := range updates {
		var newNode *nodeState
		switch update.UpdateType {
		case cc.NodeAdded:
			adds += 1
			if update.UserInitiated {
				log.Infof("NodeAdded: Reinstating offlined node %s", update.Id)
				if ns, ok := c.offlinedNodes[update.Id]; ok {
					c.nodes[update.Id] = ns
					delete(c.offlinedNodes, update.Id)
				} else {
					log.Errorf("NodeAdded: Unable to reinstate node %s, not present in offlinedNodes", update.Id)
				}
			} else if ns, ok := c.suspendedNodes[update.Id]; ok {
				if !ns.ready() {
					// Adding a node that's already suspended as non-ready, leave it in that state until ready.
					log.Infof("NodeAdded: Suspended node re-added but still awaiting readiness check %v (%s)", update.Id, ns)
				} else if ns.timeLost != nilTime {
					// This node was suspended as lost earlier, we can recover it now.
					ns.timeLost = nilTime
					c.nodes[update.Id] = ns
					delete(c.suspendedNodes, update.Id)
					log.Infof("NodeAdded: Recovered suspended node %v (%s), %s", update.Id, ns, c.status())
				} else {
					log.Infof("NodeAdded: Ignoring NodeAdded event for suspended flaky node. %v (%s)", update.Id, ns)
				}
			} else if ns, ok := c.nodes[update.Id]; !ok {
				// This is a new unrecognized node, add it to the cluster, possibly in a suspended state.
				if _, ok := c.offlinedNodes[update.Id]; ok {
					// the nodes was manually offlined, leave it offlined
					log.Infof("NodeAdded: Ignoring NodeAdded event for offlined node: %v (%#v), %s", update.Id, update.Node, c.status())
				} else if c.readyFn == nil {
					// We're not checking readiness, skip suspended state and add this as a healthy node.
					newNode = newNodeState(update.Node)
					c.nodes[update.Id] = newNode
					newNode.readyCh = nil
					log.Infof("NodeAdded: Added new node: %v (%#v), %s", update.Id, update.Node, c.status())
				} else {
					// Add this to the suspended nodes and start a goroutine to check for readiness.
					newNode = newNodeState(update.Node)
					c.suspendedNodes[update.Id] = newNode
					log.Infof("NodeAdded: Added new suspended node: %v (%#v), %s", update.Id, update.Node, c.status())
					newNode.startReadyLoop(c.readyFn)
				}
				c.nodeGroups[""].idle[update.Id] = newNode
			} else {
				// This node is already present, log this spurious add.
				log.Infof("NodeAdded: Node already added!! %v (%s)", update.Id, ns)
			}

		case cc.NodeRemoved:
			removals += 1
			if update.UserInitiated {
				log.Infof("NodeRemoved: Offlining node %s", update.Id)
				if ns, ok := c.nodes[update.Id]; ok {
					c.offlinedNodes[update.Id] = ns
					delete(c.nodes, update.Id)
				} else if ns, ok := c.suspendedNodes[update.Id]; ok {
					c.offlinedNodes[update.Id] = ns
					delete(c.suspendedNodes, update.Id)
				} else {
					log.Errorf("NodeRemoved: Unable to offline node %s, not present in nodes or suspendedNodes", update.Id)
				}
			} else if ns, ok := c.suspendedNodes[update.Id]; ok {
				// Node already suspended, make sure it's now marked as lost and not flaky (keep readiness status intact).
				log.Infof("NodeRemoved: Already suspended node marked as removed: %v (was %s)", update.Id, ns)
				ns.timeLost = time.Now()
				ns.timeFlaky = nilTime
			} else if ns, ok := c.nodes[update.Id]; ok {
				// This was a healthy node, mark it as lost now.
				ns.timeLost = time.Now()
				c.suspendedNodes[update.Id] = ns
				delete(c.nodes, update.Id)
				log.Infof("NodeRemoved: Removing node by marking as lost: %v (%s), %s", update.Id, ns, c.status())
			} else if ns, ok := c.offlinedNodes[update.Id]; ok {
				// the node was manually offlined, remove it from offlined list
				delete(c.offlinedNodes, update.Id)
				log.Infof("NodeRemoved: Removing offlined node: %v (%s), %s", update.Id, ns, c.status())
			} else {
				// We don't know about this node, log spurious remove.
				log.Infof("NodeRemoved: Cannot remove unknown node: %v", update.Id)
			}
		}
	}

	// record when we see nodes being added removed
	// also record how many times we've checked and didn't see any changes
	if adds > 0 || removals > 0 {
		log.Infof("Number of nodes added: %d\nNumber of nodes removed: %d, (%d cluster updates with no change)", adds, removals, c.nopUpdateCnt)
		c.nopUpdateCnt = 0
	} else {
		c.nopUpdateCnt++
	}

	// Clean up lost nodes that haven't recovered in time, add flaky nodes back into rotation after some time,
	// and check if newly added non-ready nodes are ready to be put into rotation.
	now := time.Now()
	for _, ns := range c.suspendedNodes {
		if ns.ready() {
			ns.readyCh = nil
		}
		if !ns.suspended() {
			// This node is initialized, remove it from suspended nodes and add it to the healthy node pool.
			// preserve its current snapshotId association
			c.nodes[ns.node.Id()] = ns
			delete(c.suspendedNodes, ns.node.Id())
			log.Infof("SuspendedNode: Node now ready, adding to rotation: %v (%s), %s", ns.node.Id(), ns, c.status())
		} else if ns.timeLost != nilTime && time.Since(ns.timeLost) > c.maxLostDuration {
			// This node has been missing too long, delete all references to it.
			delete(c.suspendedNodes, ns.node.Id())
			delete(c.nodeGroups[ns.snapshotId].idle, ns.node.Id())
			delete(c.nodeGroups[ns.snapshotId].busy, ns.node.Id())
			log.Infof("SuspendedNode: Deleting lost node: %v (%s), %s", ns.node.Id(), ns, c.status())
			// Try to notify this node's goroutine about removal so it can stop checking readiness if necessary.
			select {
			case ns.removedCh <- nil:
			default:
			}
		} else if ns.timeFlaky != nilTime && now.Sub(ns.timeFlaky) > c.maxFlakyDuration {
			// This flaky node has been suspended long enough, try adding it back to the healthy node pool.
			// We process this using the startReadyLoop/readyFn if present to reapply any side-effects,
			// but leave it with its current snapshotId association
			//
			// TimeFlaky should've been the only time* value set at this point, reset it.
			// SnapshotId must be reset since it's used in taskScheduled() and may be gone from nodeGroups.
			ns.timeFlaky = nilTime
			ns.snapshotId = ""
			if c.readyFn != nil {
				log.Infof("SuspendedNode: Reinstating flaky node momentarily: %v (%s), %s", ns.node.Id(), ns, c.status())
				ns.startReadyLoop(c.readyFn)
			} else {
				log.Infof("SuspendedNode:  Reinstating flaky node now: %v (%s), %s", ns.node.Id(), ns, c.status())
				delete(c.suspendedNodes, ns.node.Id())
				c.nodes[ns.node.Id()] = ns
			}
		}
	}

	if adds > 0 || removals > 0 {
		log.Infof("Number of nodes added: %d\nNumber of nodes removed: %d, num iterations without change: %d. %s", adds, removals, c.nopUpdateCnt, c.status())
		c.nopUpdateCnt = 0
	} else {
		c.nopUpdateCnt++
	}

	c.stats.Gauge(stats.ClusterAvailableNodes).Update(int64(len(c.nodes)))
	c.stats.Gauge(stats.ClusterFreeNodes).Update(int64(c.numFree()))
	c.stats.Gauge(stats.ClusterRunningNodes).Update(int64(c.numRunning))
	c.stats.Gauge(stats.ClusterLostNodes).Update(int64(len(c.suspendedNodes)))
}

func (c *clusterState) status() string {
	return fmt.Sprintf("now have %d healthy (%d free, %d running), and %d suspended",
		len(c.nodes), c.numFree(), c.numRunning, len(c.suspendedNodes))
}

// the following functions implement (async) user initiated onlining and offlining a node
func (c *clusterState) HasOnlineNode(nodeId cc.NodeId) bool {
	_, ok := c.nodes[nodeId]
	return ok
}

func (c *clusterState) IsOfflined(nodeId cc.NodeId) bool {
	_, ok := c.offlinedNodes[nodeId]
	return ok
}

func (c *clusterState) OnlineNode(nodeId cc.NodeId) {
	log.Infof("Onlining node %s", nodeId)
	nodeUpdate := cc.NewUserInitiatedAdd(cc.NewIdNode(string(nodeId)))
	c.nodesUpdatesCh <- []cc.NodeUpdate{nodeUpdate}
}

func (c *clusterState) OfflineNode(nodeId cc.NodeId) {
	log.Infof("Offlining node %s", nodeId)
	nodeUpdate := cc.NewUserInitiatedRemove(nodeId)
	c.nodesUpdatesCh <- []cc.NodeUpdate{nodeUpdate}
}
