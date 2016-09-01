package scheduler

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"testing"
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
	cs.UpdateCluster()
	if len(cs.nodes) != 1 {
		t.Errorf("expected cluster size to be 1")
	}

	ns, _ := cs.GetNodeState(cluster.NodeId("node1"))
	if ns.runningTask != noTask {
		t.Errorf("expected newly added node to have no tasks")
	}

	// test remove existing node
	cl.remove("node1")
	cs.UpdateCluster()
	if len(cs.nodes) != 0 {
		t.Errorf("expected cluster size to be 0")
	}
}

// ensures that removing an untracked node succeeds
func Test_ClusterState_RemoveNotTrackedNode(t *testing.T) {

	cl := makeTestCluster()
	cs := newClusterState(cl.nodes, cl.ch)

	cl.remove("node1")
	cs.UpdateCluster()
	if len(cs.nodes) != 0 {
		t.Errorf("expected cluster size to be 0")
	}
}

// ensures that adding a node more than once does not reset state
func Test_ClusterState_DuplicateNodeAdd(t *testing.T) {
	cl := makeTestCluster("node1")
	cs := newClusterState(cl.nodes, cl.ch)

	cs.TaskScheduled("node1", "task1")

	// readd node to cluster
	cl.add("node1")
	cs.UpdateCluster()

	// verify cluster is still size1
	if len(cs.nodes) != 1 {
		t.Errorf("Expected cluster size to be 1")
	}

	ns, _ := cs.GetNodeState("node1")
	// verify that the state wasn't modified
	if ns.runningTask != "task1" {
		t.Errorf("Expected adding an already tracked node to not modify state %v", cs.nodes[cluster.NodeId("node1")].runningTask)
	}
}

func Test_TaskStarted(t *testing.T) {
	cl := makeTestCluster("node1")
	cs := newClusterState(cl.nodes, cl.ch)

	cs.TaskScheduled("node1", "task1")
	ns, _ := cs.GetNodeState("node1")

	if ns.runningTask != "task1" {
		t.Errorf("Expected Node1 to be running task1")
	}
}

func Test_TaskCompleted(t *testing.T) {
	cl := makeTestCluster("node1")
	cs := newClusterState(cl.nodes, cl.ch)

	cs.TaskScheduled("node1", "task1")
	ns, _ := cs.GetNodeState("node1")

	cs.TaskCompleted("node1", "task1")
	if ns.runningTask != noTask {
		t.Errorf("Expected Node1 to be running task1")
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
