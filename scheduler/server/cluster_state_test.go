package server

import (
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/wisechengyi/scoot/cloud/cluster"
	cc "github.com/wisechengyi/scoot/cloud/cluster"
	"github.com/wisechengyi/scoot/common"
	"github.com/wisechengyi/scoot/common/stats"
)

// ensures nodes can be added and removed
func Test_ClusterState_UpdateCluster(t *testing.T) {
	cs, nodesUpdatesCh, _ := setupTestClusterState(nil)

	if len(cs.nodes) != 0 {
		t.Errorf("expected cluster size to be 0")
	}

	// test add node
	addNode("node1", nodesUpdatesCh)
	cs.updateCluster()
	if len(cs.nodes) != 1 {
		t.Errorf("expected cluster size to be 1")
	}
	if len(cs.nodeGroups[""].idle) != 1 {
		t.Errorf("expected clusterGroup[].idle size to be 1")
	}

	ns, _ := cs.getNodeState(cluster.NodeId("node1"))
	if ns.runningTask != noTask {
		t.Errorf("expected newly added node to have no tasks")
	}

	// test remove existing node
	removeNode("node1", nodesUpdatesCh)
	cs.updateCluster()
	if len(cs.nodes) != 0 {
		t.Errorf("expected cluster size to be 0")
	}
	if len(cs.nodeGroups[""].idle) != 1 || cs.suspendedNodes["node1"] == nil {
		t.Errorf("expected clusterGroup[].idle size to be 1 and lostNodes.node1 to exist.")
	}
}

// ensures that removing an untracked node succeeds
func Test_ClusterState_RemoveNotTrackedNode(t *testing.T) {
	cs, nodesUpdatesCh, _ := setupTestClusterState(nil)

	removeNode("node1", nodesUpdatesCh)
	cs.updateCluster()
	if len(cs.nodes) != 0 {
		t.Errorf("expected cluster size to be 0")
	}
}

// ensures that adding a node more than once does not reset state
func Test_ClusterState_DuplicateNodeAdd(t *testing.T) {
	cs, nodesUpdatesCh, _ := setupTestClusterState(nil, "node1")

	cs.taskScheduled("node1", "job1", "task1", "")

	// readd node to cluster
	addNode("node1", nodesUpdatesCh)
	cs.updateCluster()

	// verify cluster is still size1
	if len(cs.nodes) != 1 {
		t.Errorf("Expected cluster size to be 1")
	}

	ns, _ := cs.getNodeState("node1")
	// verify that the state wasn't modified
	if ns.runningTask != "task1" {
		t.Errorf("Expected adding an already tracked node to not modify state %v", cs.nodes[cluster.NodeId("node1")].runningTask)
	}
}

func Test_ClusterState_OfflineNode(t *testing.T) {
	nodeID := "node1"
	cNodeId := cluster.NodeId(nodeID)
	cs, _, _ := setupTestClusterState(nil, nodeID)
	if _, ok := cs.nodes[cNodeId]; !ok {
		t.Errorf("Expected %s to be in cs.nodes", nodeID)
	}
	if len(cs.nodes) != 1 {
		t.Errorf("Expected len(cs.nodes) to be 1, was %d", len(cs.nodes))
	}
	cs.OfflineNode(cNodeId)
	cs.updateCluster()
	if _, ok := cs.nodes[cNodeId]; ok {
		t.Errorf("Expected %s to be offlined", nodeID)
	}
	if len(cs.nodes) != 0 {
		t.Errorf("Expected len(cs.nodes) to be 0, was %d", len(cs.nodes))
	}
	if _, ok := cs.offlinedNodes[cNodeId]; !ok {
		t.Errorf("Expected node %s to be in offline map: %v", nodeID, cs.offlinedNodes)
	}
}

func Test_ClusterState_ReinstateNode(t *testing.T) {
	nodeID := "node1"
	cNodeId := cluster.NodeId(nodeID)
	cs, _, _ := setupTestClusterState(nil, nodeID)
	if _, ok := cs.nodes[cNodeId]; !ok {
		t.Errorf("Expected %s to be in cs.nodes", nodeID)
	}
	if len(cs.nodes) != 1 {
		t.Errorf("Expected len(cs.nodes) to be 1, was %d", len(cs.nodes))
	}

	// offline the node
	cs.OfflineNode(cNodeId)
	cs.updateCluster()
	if _, ok := cs.nodes[cNodeId]; ok {
		t.Errorf("Expected %s to be offlined", cNodeId)
	}
	if len(cs.nodes) != 0 {
		t.Errorf("Expected len(cs.nodes) to be 0, was %d", len(cs.nodes))
	}
	if _, ok := cs.offlinedNodes[cNodeId]; !ok {
		t.Errorf("Expected node %s to be in offline map: %v", cNodeId, cs.offlinedNodes)
	}

	// online the node and verify it is no longer offlined
	cs.OnlineNode(cNodeId)
	cs.updateCluster()
	if _, ok := cs.nodes[cNodeId]; !ok {
		t.Errorf("Expected %s to be in cs.nodes after reinstatement", nodeID)
	}
	if len(cs.nodes) != 1 {
		t.Errorf("Expected len(cs.nodes) to be 1, was %d", len(cs.nodes))
	}
	if _, ok := cs.offlinedNodes[cNodeId]; ok {
		t.Errorf("Expected node %s to not be in offline map: %v", nodeID, cs.offlinedNodes)
	}
}

func Test_ClusterState_OfflineNodeAlreadyOffline(t *testing.T) {
	nodeID := "node1"
	cNodeId := cluster.NodeId(nodeID)
	cs, _, _ := setupTestClusterState(nil, nodeID)
	if _, ok := cs.nodes[cNodeId]; !ok {
		t.Errorf("Expected %s to be in cs.nodes", nodeID)
	}
	if len(cs.nodes) != 1 {
		t.Errorf("Expected len(cs.nodes) to be 1, was %d", len(cs.nodes))
	}

	// offline the node
	cs.OfflineNode(cNodeId)
	cs.updateCluster()
	if _, ok := cs.nodes[cNodeId]; ok {
		t.Errorf("Expected %s to be offlined", nodeID)
	}
	if len(cs.nodes) != 0 {
		t.Errorf("Expected len(cs.nodes) to be 0, was %d", len(cs.nodes))
	}
	if _, ok := cs.offlinedNodes[cNodeId]; !ok {
		t.Errorf("Expected node %s to be in offline map: %v", nodeID, cs.offlinedNodes)
	}

	// offline the node
	cs.OfflineNode(cNodeId)
	cs.updateCluster()
	if _, ok := cs.nodes[cluster.NodeId(nodeID)]; ok {
		t.Errorf("Expected %s to still be offlined", nodeID)
	}
	if len(cs.nodes) != 0 {
		t.Errorf("Expected len(cs.nodes) to be 0, was %d", len(cs.nodes))
	}
	if len(cs.offlinedNodes) != 1 {
		t.Errorf("Expected num off line nodes to be 0, was %d", len(cs.offlinedNodes))
	}
	if _, ok := cs.offlinedNodes[cNodeId]; !ok {
		t.Errorf("Expected node %s to be in offline map: %v", nodeID, cs.offlinedNodes)
	}
}

func Test_ClusterState_ReinstateNodeAlreadyReinstated(t *testing.T) {
	nodeID := "node1"
	cNodeId := cluster.NodeId(nodeID)
	cs, _, _ := setupTestClusterState(nil, nodeID)
	if _, ok := cs.nodes[cNodeId]; !ok {
		t.Errorf("Expected %s to be in cs.nodes", nodeID)
	}
	if len(cs.nodes) != 1 {
		t.Errorf("Expected len(cs.nodes) to be 1, was %d", len(cs.nodes))
	}

	// offline the node
	cs.OfflineNode(cNodeId)
	cs.updateCluster()
	if _, ok := cs.nodes[cluster.NodeId(nodeID)]; ok {
		t.Errorf("Expected %s to be offlined", nodeID)
	}
	if len(cs.nodes) != 0 {
		t.Errorf("Expected len(cs.nodes) to be 0, was %d", len(cs.nodes))
	}

	// offline the node
	cs.OnlineNode(cNodeId)
	cs.updateCluster()
	if _, ok := cs.nodes[cNodeId]; !ok {
		t.Errorf("Expected %s to be in cs.nodes after reinstatement", nodeID)
	}
	if len(cs.nodes) != 1 {
		t.Errorf("Expected len(cs.nodes) to be 1, was %d", len(cs.nodes))
	}

	cs.OnlineNode(cNodeId)
	cs.updateCluster()
	if _, ok := cs.nodes[cluster.NodeId(nodeID)]; !ok {
		t.Errorf("Expected %s to still be in cs.nodes after double reinstatement", nodeID)
	}
	if len(cs.nodes) != 1 {
		t.Errorf("Expected len(cs.nodes) to still be 1, was %d", len(cs.nodes))
	}
	if _, ok := cs.offlinedNodes[cNodeId]; ok {
		t.Errorf("Expected node %s to not be in offline map: %v", nodeID, cs.offlinedNodes)
	}

}

func Test_ClusterState_TaskStarted(t *testing.T) {
	cs, _, _ := setupTestClusterState(nil, "node1")

	cs.taskScheduled("node1", "job1", "task1", "")
	ns, _ := cs.getNodeState("node1")

	if ns.runningTask != "task1" {
		t.Errorf("Expected Node1 to be running task1")
	}
}

func Test_ClusterState_TaskCompleted(t *testing.T) {
	cs, _, _ := setupTestClusterState(nil, "node1")

	cs.taskScheduled("node1", "job1", "task1", "")
	ns, _ := cs.getNodeState("node1")

	cs.taskCompleted("node1", false)
	if ns.runningTask != noTask {
		t.Errorf("Expected Node1 to be running task1")
	}

}

// verify that available and suspended maps are populated correctly when a node becomes
// unhealthy after task completion
func Test_ClusterState_NodeUnhealthy(t *testing.T) {
	readyCh := make(chan interface{})
	readyFn := func(node cluster.Node) (bool, time.Duration) {
		return true, time.Duration(0)
	}

	setReady := func(node string) {
		close(readyCh)
	}

	//setup cluster with 1 node and mark it as reaady
	cs, _, statsRegistry := setupTestClusterState(readyFn, "node1")
	setReady("node1")
	time.Sleep(2 * time.Millisecond)
	cs.updateCluster()

	// verify that node is added and ready.
	if len(cs.nodes) != 1 || len(cs.suspendedNodes) != 0 {
		t.Fatalf("Expected empty nodes and suspendedNodes, got: %s, %s",
			spew.Sdump(cs.nodes), spew.Sdump(cs.suspendedNodes))
	}
	if !stats.StatsOk("1st stats check:", statsRegistry, t,
		map[string]stats.Rule{
			stats.ClusterAvailableNodes: {Checker: stats.Int64EqTest, Value: 1},
			stats.ClusterFreeNodes:      {Checker: stats.Int64EqTest, Value: 1},
			stats.ClusterLostNodes:      {Checker: stats.Int64EqTest, Value: 0},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// schedule a task on the node
	cs.taskScheduled("node1", "job1", "task1", "snapA")

	// set ready function to return false to mock unhealthy worker behavior
	cs.readyFn = func(node cluster.Node) (bool, time.Duration) {
		return false, time.Duration(0)
	}

	// task finished
	cs.taskCompleted("node1", false)
	if _, ok := cs.nodes["node1"]; ok {
		t.Fatalf("Unhealthy node was not moved out of cs.nodes")
	} else if _, ok := cs.suspendedNodes["node1"]; !ok {
		t.Fatalf("Unhealthy node was not moved into cs.suspendedNodes")
	} else if cs.suspendedNodes["node1"].timeLost == nilTime {
		t.Fatalf("Unhealthy nodes should record the time they were marked lost")
	}

	// verify stats
	time.Sleep(2 * time.Millisecond)
	cs.updateCluster()
	if !stats.StatsOk("1st stats check:", statsRegistry, t,
		map[string]stats.Rule{
			stats.ClusterAvailableNodes: {Checker: stats.Int64EqTest, Value: 0},
			stats.ClusterFreeNodes:      {Checker: stats.Int64EqTest, Value: 0},
			stats.ClusterLostNodes:      {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}
}

// verify that idle and busy maps are populated correctly and that flaky/lost/init'd status are as well.
func Test_ClusterState_NodeGroups(t *testing.T) {
	// use a map of ready channels, one channel for each node
	ready := map[string]chan interface{}{
		"node1": make(chan interface{}), "node2": make(chan interface{}),
		"node3": make(chan interface{}), "node4": make(chan interface{}),
	}
	readyFn := func(node cluster.Node) (bool, time.Duration) {
		select {
		case <-ready[string(node.Id())]:
			return true, time.Duration(0)
		default:
			return false, time.Millisecond
		}
	}
	setReady := func(node string) {
		close(ready[node])
	}
	cs, nodesUpdatesCh, statsRegistry := setupTestClusterState(readyFn, "node1", "node2", "node3", "node4")

	// Test that nodes are added in the suspended state.
	if len(cs.nodes) != 0 || len(cs.suspendedNodes) != 4 {
		t.Fatalf("Expected empty nodes and populated suspendedNodes, got: %s, %s",
			spew.Sdump(cs.nodes), spew.Sdump(cs.suspendedNodes))
	}

	// Set node1 to init'd and make sure it gets moved out of suspended.
	// Sleeping so nodeState goroutines can pick up readiness changes.
	node1 := cs.suspendedNodes[cluster.NodeId("node1")]
	setReady("node1")
	time.Sleep(10 * time.Millisecond)
	cs.updateCluster()
	if len(cs.nodes) != 1 || len(cs.suspendedNodes) != 3 || node1.suspended() {
		t.Fatalf("Expected healthy node1 in nodes and the rest in suspendedNodes, got\n:nodes:%s\nsuspended:%s",
			spew.Sdump(cs.nodes), spew.Sdump(cs.suspendedNodes))
	}

	if !stats.StatsOk("1st stats check:", statsRegistry, t,
		map[string]stats.Rule{
			stats.ClusterAvailableNodes: {Checker: stats.Int64EqTest, Value: 1},
			stats.ClusterFreeNodes:      {Checker: stats.Int64EqTest, Value: 1},
			stats.ClusterLostNodes:      {Checker: stats.Int64EqTest, Value: 3},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// Remove node2 and make sure its state is set correctly.
	node2 := cs.suspendedNodes[cluster.NodeId("node2")]
	removeNode("node2", nodesUpdatesCh)
	cs.updateCluster()
	if len(cs.nodes) != 1 || len(cs.suspendedNodes) != 3 || node2.readyCh == nil || node2.timeLost == nilTime {
		t.Fatalf("Expected both uninitialized and lost status for node2, got: %s, %s",
			spew.Sdump(cs.nodes), spew.Sdump(cs.suspendedNodes))
	}

	// Set node2 init'd and make sure it's still suspended as lost
	setReady("node2")
	time.Sleep(10 * time.Millisecond)
	cs.updateCluster()
	if len(cs.nodes) != 1 || len(cs.suspendedNodes) != 3 || node2.readyCh != nil || node2.timeLost == nilTime {
		t.Fatalf("Expected both init'd and lost status for node2, got: %s, %s",
			spew.Sdump(cs.nodes), spew.Sdump(cs.suspendedNodes))
	}

	if !stats.StatsOk("2nd stats check:", statsRegistry, t,
		map[string]stats.Rule{
			stats.ClusterAvailableNodes: {Checker: stats.Int64EqTest, Value: 1},
			stats.ClusterFreeNodes:      {Checker: stats.Int64EqTest, Value: 1},
			stats.ClusterLostNodes:      {Checker: stats.Int64EqTest, Value: 3},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// Re-add node2 and set the rest as init'd, then check nodes/suspendedNodes.
	addNode("node2", nodesUpdatesCh)
	cs.updateCluster()
	setReady("node3")
	setReady("node4")
	time.Sleep(10 * time.Millisecond)
	cs.updateCluster()
	if len(cs.nodes) != 4 || len(cs.suspendedNodes) != 0 ||
		cs.nodes[cluster.NodeId("node1")].suspended() ||
		cs.nodes[cluster.NodeId("node2")].suspended() ||
		cs.nodes[cluster.NodeId("node3")].suspended() ||
		cs.nodes[cluster.NodeId("node4")].suspended() {
		t.Fatalf("Expected all healthy and init'd nodes, got: %s, %s",
			spew.Sdump(cs.nodes), spew.Sdump(cs.suspendedNodes))
	}

	if !stats.StatsOk("3rd stats check:", statsRegistry, t,
		map[string]stats.Rule{
			stats.ClusterAvailableNodes: {Checker: stats.Int64EqTest, Value: 4},
			stats.ClusterFreeNodes:      {Checker: stats.Int64EqTest, Value: 4},
			stats.ClusterLostNodes:      {Checker: stats.Int64EqTest, Value: 0},
		}) {
		t.Fatal("stats check did not pass.")
	}
	// Test the the right idle/busy maps are filled out for each snapshotId.
	cs.taskScheduled("node1", "job1", "task1", "snapA")
	cs.taskScheduled("node2", "job1", "task2", "snapA")
	cs.taskScheduled("node3", "job1", "task3", "snapB")
	expectedGroups := map[string]*nodeGroup{
		"": {
			idle: map[cluster.NodeId]*nodeState{
				"node4": cs.nodes["node4"],
			},
			busy: map[cluster.NodeId]*nodeState{},
		},
		"snapA": {
			idle: map[cluster.NodeId]*nodeState{},
			busy: map[cluster.NodeId]*nodeState{
				"node1": cs.nodes["node1"],
				"node2": cs.nodes["node2"],
			},
		},
		"snapB": {
			idle: map[cluster.NodeId]*nodeState{},
			busy: map[cluster.NodeId]*nodeState{
				"node3": cs.nodes["node3"],
			},
		},
	}
	if !reflect.DeepEqual(cs.nodeGroups, expectedGroups) {
		t.Fatalf("Expected: %s\nGot: %s", spew.Sdump(expectedGroups), spew.Sdump(cs.nodeGroups))
	}

	// Test that finishing a jobs moves it to the idle list for its snapshotId.
	cs.taskCompleted("node1", false)
	expectedGroups["snapA"].idle["node1"] = cs.nodes["node1"]
	delete(expectedGroups["snapA"].busy, "node1")
	if !reflect.DeepEqual(cs.nodeGroups, expectedGroups) {
		t.Fatalf("Expected: %s\nGot: %s", spew.Sdump(expectedGroups), spew.Sdump(cs.nodeGroups))
	}

	// Test the rescheduling a task moves it correctly from an idle list to a busy one.
	cs.taskScheduled("node1", "job1", "task1", "snapB")
	expectedGroups["snapB"].busy["node1"] = cs.nodes["node1"]
	delete(expectedGroups["snapA"].idle, "node1")
	if !reflect.DeepEqual(cs.nodeGroups, expectedGroups) {
		t.Fatalf("Expected: %s\nGot: %s", spew.Sdump(expectedGroups), spew.Sdump(cs.nodeGroups))
	}

	// Task finished and is marked as flaky
	cs.taskCompleted("node1", true)
	if _, ok := cs.nodes["node1"]; ok {
		t.Fatalf("Flaky node was not moved out of cs.nodes")
	} else if _, ok := cs.suspendedNodes["node1"]; !ok {
		t.Fatalf("Flaky node was not moved into cs.suspendedNodes")
	} else if cs.suspendedNodes["node1"].timeFlaky == nilTime {
		t.Fatalf("Flaky nodes should record the time they were marked flaky")
	}

	// Nodes are removed and marked as lost
	removeNode("node2", nodesUpdatesCh)
	cs.updateCluster()
	removeNode("node3", nodesUpdatesCh)
	cs.updateCluster()
	if _, ok := cs.nodes["node2"]; ok {
		t.Fatalf("Lost node was not moved out of cs.nodes")
	} else if _, ok := cs.suspendedNodes["node2"]; !ok {
		t.Fatalf("Lost node was not moved into cs.suspendedNodes")
	} else if cs.suspendedNodes["node2"].timeLost == nilTime {
		t.Fatalf("Lost nodes should record the time they were marked lost")
	}
	if _, ok := cs.nodes["node3"]; ok {
		t.Fatalf("Lost node was not moved out of cs.nodes")
	} else if _, ok := cs.suspendedNodes["node3"]; !ok {
		t.Fatalf("Lost node was not moved into cs.suspendedNodes")
	} else if cs.suspendedNodes["node3"].timeLost == nilTime {
		t.Fatalf("Lost nodes should record the time they were marked lost")
	}

	// Make sure suspended nodes are either discarded or reinstated as appropriate.
	cs.maxLostDuration = time.Millisecond
	cs.maxFlakyDuration = time.Millisecond
	addNode("node3", nodesUpdatesCh)
	time.Sleep(2 * time.Millisecond)
	cs.updateCluster()

	if _, ok := cs.suspendedNodes["node3"]; ok {
		t.Fatalf("revived node3 should have been removed from the suspended list")
	} else if ns, ok := cs.nodes["node3"]; !ok {
		t.Fatalf("revived node3 should have been reinstated")
	} else if ns.timeLost != nilTime {
		t.Fatalf("revived node3 should've been marked not lost")
	}

	if _, ok := cs.nodes["node2"]; ok {
		t.Fatalf("lost node2 should not have been moved to cs.nodes")
	} else if _, ok := cs.suspendedNodes["node2"]; ok {
		t.Fatalf("lost node2 should have been removed from suspended list")
	}

	// Reinstating nodes is a two-step process, update the cluster one more time.
	time.Sleep(2 * time.Millisecond)
	cs.updateCluster()
	if _, ok := cs.nodes["node1"]; !ok {
		t.Fatalf("flaky node1 should have been moved to cs.nodes")
	} else if _, ok := cs.suspendedNodes["node1"]; ok {
		t.Fatalf("flaky node1 should have been removed from suspended list")
	}

}

func initTestCluster(nodesUpdateCh chan []cc.NodeUpdate, nodes ...string) {
	nodeUpdates := []cc.NodeUpdate{}
	for _, n := range nodes {
		nodeUpdates = append(nodeUpdates, cc.NewAdd(cc.NewIdNode(n)))
	}
	nodesUpdateCh <- nodeUpdates
}

func setupTestClusterState(rfn ReadyFn, nodes ...string) (*clusterState, chan []cc.NodeUpdate, stats.StatsRegistry) {
	statsRegistry := stats.NewFinagleStatsRegistry()
	statsReceiver, _ := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsRegistry }, 0)

	nodeUpdateCh := make(chan []cc.NodeUpdate, common.DefaultClusterChanSize)
	cs := newClusterState(nodeUpdateCh, rfn, statsReceiver)
	initTestCluster(nodeUpdateCh, nodes...)
	cs.updateCluster()
	return cs, nodeUpdateCh, statsRegistry
}

func addNode(node string, nodeUpdateCh chan []cc.NodeUpdate) {
	update := cluster.NewAdd(cluster.NewIdNode(node))
	nodeUpdateCh <- []cc.NodeUpdate{update}
}

func removeNode(node string, nodeUpdateCh chan []cc.NodeUpdate) {
	update := cluster.NewRemove(cluster.NodeId(node))
	nodeUpdateCh <- []cc.NodeUpdate{update}
}
