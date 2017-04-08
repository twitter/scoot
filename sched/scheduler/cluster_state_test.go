package scheduler

import (
	"reflect"
	"testing"
	"time"

	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/cloud/cluster"
)

// ensures nodes can be added and removed
func Test_ClusterState_UpdateCluster(t *testing.T) {

	cl := makeTestCluster()
	cs := newClusterState(cl.nodes, cl.ch)

	if len(cs.nodes) != 0 {
		t.Errorf("expected cluster size to be 0")
	}

	// test add node
	cl.add("node1")
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
	cl.remove("node1")
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

	cl := makeTestCluster()
	cs := newClusterState(cl.nodes, cl.ch)

	cl.remove("node1")
	cs.updateCluster()
	if len(cs.nodes) != 0 {
		t.Errorf("expected cluster size to be 0")
	}
}

// ensures that adding a node more than once does not reset state
func Test_ClusterState_DuplicateNodeAdd(t *testing.T) {
	cl := makeTestCluster("node1")
	cs := newClusterState(cl.nodes, cl.ch)

	cs.taskScheduled("node1", "task1", "")

	// readd node to cluster
	cl.add("node1")
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

func Test_TaskStarted(t *testing.T) {
	cl := makeTestCluster("node1")
	cs := newClusterState(cl.nodes, cl.ch)

	cs.taskScheduled("node1", "task1", "")
	ns, _ := cs.getNodeState("node1")

	if ns.runningTask != "task1" {
		t.Errorf("Expected Node1 to be running task1")
	}
}

func Test_TaskCompleted(t *testing.T) {
	cl := makeTestCluster("node1")
	cs := newClusterState(cl.nodes, cl.ch)

	cs.taskScheduled("node1", "task1", "")
	ns, _ := cs.getNodeState("node1")

	cs.taskCompleted("node1", "task1", false)
	if ns.runningTask != noTask {
		t.Errorf("Expected Node1 to be running task1")
	}

}

// verify that idle and busy maps are populated correctly.
func Test_NodeGroups(t *testing.T) {
	cl := makeTestCluster("node1", "node2", "node3", "node4")
	cs := newClusterState(cl.nodes, cl.ch)

	// Test the the right idle/busy maps are filled out for each snapshotId.
	cs.taskScheduled("node1", "task1", "snapA")
	cs.taskScheduled("node2", "task2", "snapA")
	cs.taskScheduled("node3", "task3", "snapB")
	expectedGroups := map[string]*nodeGroup{
		"": &nodeGroup{
			idle: map[cluster.NodeId]*nodeState{
				"node4": cs.nodes["node4"],
			},
			busy: map[cluster.NodeId]*nodeState{},
		},
		"snapA": &nodeGroup{
			idle: map[cluster.NodeId]*nodeState{},
			busy: map[cluster.NodeId]*nodeState{
				"node1": cs.nodes["node1"],
				"node2": cs.nodes["node2"],
			},
		},
		"snapB": &nodeGroup{
			idle: map[cluster.NodeId]*nodeState{},
			busy: map[cluster.NodeId]*nodeState{
				"node3": cs.nodes["node3"],
			},
		},
	}
	if !reflect.DeepEqual(cs.nodeGroups, expectedGroups) {
		t.Errorf("Expected: %v\nGot: %v", render.Render(expectedGroups), render.Render(cs.nodeGroups))
	}

	// Test that finishing a jobs moves it to the idle list for its snapshotId.
	cs.taskCompleted("node1", "task1", false)
	expectedGroups["snapA"].idle["node1"] = cs.nodes["node1"]
	delete(expectedGroups["snapA"].busy, "node1")
	if !reflect.DeepEqual(cs.nodeGroups, expectedGroups) {
		t.Errorf("Expected: %v\nGot: %v", render.Render(expectedGroups), render.Render(cs.nodeGroups))
	}

	// Test the rescheduling a task moves it correctly from an idle list to a busy one.
	cs.taskScheduled("node1", "task1", "snapB")
	expectedGroups["snapB"].busy["node1"] = cs.nodes["node1"]
	delete(expectedGroups["snapA"].idle, "node1")
	if !reflect.DeepEqual(cs.nodeGroups, expectedGroups) {
		t.Errorf("Expected: %v\nGot: %v", render.Render(expectedGroups), render.Render(cs.nodeGroups))
	}

	// Task finished and is marked as flaky
	cs.taskCompleted("node1", "task1", true)
	if _, ok := cs.nodes["node1"]; ok {
		t.Errorf("Flaky node was not moved out of cs.nodes")
	} else if _, ok := cs.suspendedNodes["node1"]; !ok {
		t.Errorf("Flaky node was not moved into cs.suspendedNodes")
	} else if cs.suspendedNodes["node1"].timeFlaky == nilTime {
		t.Errorf("Flaky nodes should record the time they were marked flaky")
	}

	// Nodes are removed and marked as lost
	cl.remove("node2")
	cs.updateCluster()
	cl.remove("node3")
	cs.updateCluster()
	if _, ok := cs.nodes["node2"]; ok {
		t.Errorf("Lost node was not moved out of cs.nodes")
	} else if _, ok := cs.suspendedNodes["node2"]; !ok {
		t.Errorf("Lost node was not moved into cs.suspendedNodes")
	} else if cs.suspendedNodes["node2"].timeLost == nilTime {
		t.Errorf("Lost nodes should record the time they were marked lost")
	}
	if _, ok := cs.nodes["node3"]; ok {
		t.Errorf("Lost node was not moved out of cs.nodes")
	} else if _, ok := cs.suspendedNodes["node3"]; !ok {
		t.Errorf("Lost node was not moved into cs.suspendedNodes")
	} else if cs.suspendedNodes["node3"].timeLost == nilTime {
		t.Errorf("Lost nodes should record the time they were marked lost")
	}

	// Make sure suspended nodes are either discarded or reinstated as appropriate.
	cs.maxLostDuration = time.Millisecond
	cs.maxFlakyDuration = time.Millisecond
	cl.add("node3")
	time.Sleep(2 * time.Millisecond)
	cs.updateCluster()
	if _, ok := cs.suspendedNodes["node3"]; ok {
		t.Errorf("revived node3 should have been removed from the suspended list")
	} else if ns, ok := cs.nodes["node3"]; !ok {
		t.Errorf("revived node3 should have been reinstated")
	} else if ns.timeLost != nilTime {
		t.Errorf("revived node3 should've been marked not lost")
	}
	if _, ok := cs.nodes["node1"]; !ok {
		t.Errorf("flaky node1 should have been moved to cs.nodes")
	} else if _, ok := cs.suspendedNodes["node1"]; ok {
		t.Errorf("flaky node1 should have been removed from suspended list")
	}
	if _, ok := cs.nodes["node2"]; ok {
		t.Errorf("lost node2 should not have been moved to cs.nodes")
	} else if _, ok := cs.suspendedNodes["node2"]; ok {
		t.Errorf("lost node2 should have been removed from suspended list")
	}
}

type testCluster struct {
	ch    chan []cluster.NodeUpdate
	nodes []cluster.Node
}

func makeTestCluster(node ...string) *testCluster {
	h := &testCluster{
		ch: make(chan []cluster.NodeUpdate, 1),
	}
	nodes := []cluster.Node{}
	for _, n := range node {
		nodes = append(nodes, cluster.NewIdNode(n))
	}
	h.nodes = nodes
	return h
}

func (h *testCluster) add(node string) {
	update := cluster.NewAdd(cluster.NewIdNode(node))
	h.ch <- []cluster.NodeUpdate{update}
}

func (h *testCluster) remove(node string) {
	update := cluster.NewRemove(cluster.NodeId(node))
	h.ch <- []cluster.NodeUpdate{update}
}
