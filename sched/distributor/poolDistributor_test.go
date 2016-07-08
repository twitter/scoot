package distributor

import (
	"github.com/golang/mock/gomock"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	"strings"
	"testing"
)

func TestPoolDistributor_NilCluster(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	clusterMock := cm.NewMockCluster(mockCtrl)
	clusterMock.EXPECT().Members().Return(nil)

	dist := NewPoolDistributor(clusterMock)

	if nil != dist {
		t.Error("Expected dist to be nil when no members in Cluster")
	}
}

func TestPoolDistributor_EmptyCluster(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	nodes := make([]cm.Node, 0)

	clusterMock := cm.NewMockCluster(mockCtrl)
	clusterMock.EXPECT().Members().Return(nodes)

	dist := NewPoolDistributor(clusterMock)

	if nil != dist {
		t.Error("Expected dist to be nil when no members in Cluster")
	}
}

func TestPoolDistributor_OneNodeCluster(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	nodes := cm.GenerateTestNodes(1)
	clusterMock := cm.NewMockCluster(mockCtrl)
	clusterMock.EXPECT().Members().Return(nodes)

	dist := NewPoolDistributor(clusterMock)

	if nil == dist {
		t.Error("Expected dist to not be nil when 1 or more nodes in Cluster")
	}

	node1 := dist.ReserveNode()

	select {
	case <-dist.freeCh:
		t.Error("Expected cluster to be full schedlued")
	default:
	}

	dist.ReleaseNode(node1)
	node2 := dist.ReserveNode()

	if node1.Id() != node2.Id() {
		t.Error("Expected nodes to be the same Id since there is only one in the cluster")
	}

	select {
	case <-dist.freeCh:
		t.Error("Expected cluster to be full schedlued")
	default:
	}
}

func TestPoolDynamicDistributor_AddNodes(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	nodes := cm.GenerateTestNodes(1)
	updateCh := make(chan cm.NodeUpdate)

	clusterState := cm.DynamicClusterState{
		InitialMembers: nodes,
		Updates:        updateCh,
	}

	dist := NewDynamicPoolDistributor(clusterState)
	node1 := dist.ReserveNode()

	if strings.Compare(node1.Id(), nodes[0].Id()) != 0 {
		t.Error("Unexpected Node Returned", node1.Id(), nodes[0].Id())
	}

	select {
	case <-dist.freeCh:
		t.Error("Expected cluster to be full schedlued")
	default:
	}

	addedNode := (cm.GenerateTestNodes(1))[0]

	updateCh <- cm.NodeUpdate{
		Node:       addedNode,
		UpdateType: cm.NodeAdded,
	}

	node2 := dist.ReserveNode()

	if strings.Compare(node2.Id(), addedNode.Id()) != 0 {
		t.Error("Unexpected Node Returned", node2.Id(), addedNode)
	}

	select {
	case <-dist.freeCh:
		t.Error("Expected cluster to be full schedlued")
	default:
	}
}

func TestDynamicDistributor_RemoveNodesUnReserved(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	nodes := cm.GenerateTestNodes(2)
	updateCh := make(chan cm.NodeUpdate)

	clusterState := cm.DynamicClusterState{
		InitialMembers: nodes,
		Updates:        updateCh,
	}

	dist := NewDynamicPoolDistributor(clusterState)

	updateCh <- cm.NodeUpdate{
		Node:       nodes[0],
		UpdateType: cm.NodeRemoved,
	}

	node1 := dist.ReserveNode()

	if strings.Compare(node1.Id(), nodes[0].Id()) == 0 {
		t.Error("Removed Node Retured", nodes[0].Id())
	}

	if strings.Compare(node1.Id(), nodes[1].Id()) != 0 {
		t.Error("Unexpected Node Returned", node1.Id(), nodes[1].Id())
	}

	select {
	case <-dist.freeCh:
		t.Error("Expected cluster to be full schedlued")
	default:
	}
}

func TestDynamicDistributor_RemoveNodesReserved(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	nodes := cm.GenerateTestNodes(2)
	updateCh := make(chan cm.NodeUpdate)

	clusterState := cm.DynamicClusterState{
		InitialMembers: nodes,
		Updates:        updateCh,
	}

	dist := NewDynamicPoolDistributor(clusterState)

	// fully schedule the cluster
	node1 := dist.ReserveNode()
	node2 := dist.ReserveNode()

	// remove node0 from cluster
	updateCh <- cm.NodeUpdate{
		Node:       nodes[0],
		UpdateType: cm.NodeRemoved,
	}

	// return node1 & node2, node1 should get removed
	dist.ReleaseNode(node1)
	dist.ReleaseNode(node2)

	node3 := dist.ReserveNode()

	if strings.Compare(node3.Id(), nodes[1].Id()) != 0 {
		t.Error("Unexpected Node Returned", node3.Id(), nodes[1].Id())
	}

	select {
	case <-dist.freeCh:
		t.Error("Expected cluster to be full schedlued")
	default:
	}
}

func TestDynamicDistributor_ReAddedRemovedNodeIsAvailable(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	nodes := cm.GenerateTestNodes(1)
	updateCh := make(chan cm.NodeUpdate)

	clusterState := cm.DynamicClusterState{
		InitialMembers: nodes,
		Updates:        updateCh,
	}

	dist := NewDynamicPoolDistributor(clusterState)

	// mimic flapping node remove and then add
	updateCh <- cm.NodeUpdate{
		Node:       nodes[0],
		UpdateType: cm.NodeRemoved,
	}

	updateCh <- cm.NodeUpdate{
		Node:       nodes[0],
		UpdateType: cm.NodeAdded,
	}

	select {
	case <-dist.freeCh:

	default:
		t.Error("Expected node to be available after remove then add")
	}

}
