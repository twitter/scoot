package distributor

import (
	"github.com/golang/mock/gomock"
	"github.com/scootdev/scoot/sched"
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

	job1 := sched.Job{
		Id: "job1",
	}
	job2 := sched.Job{
		Id: "job2",
	}

	node1 := dist.ReserveNode(job1)

	select {
	case <-dist.freeCh:
		t.Error("Expected cluster to be full schedlued")
	default:
	}

	dist.ReleaseNode(node1)
	node2 := dist.ReserveNode(job2)

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

	nodes := cm.GenerateTestNodes(2)
	updateCh := make(chan cm.NodeUpdate)

	clusterMock := cm.NewMockDynamicCluster(mockCtrl)
	clusterMock.EXPECT().Members().Return([]cm.Node{nodes[0]})
	clusterMock.EXPECT().NodeUpdates().Return(updateCh)

	dist := NewDynamicPoolDistributor(clusterMock)

	job1 := sched.Job{
		Id: "job1",
	}
	node1 := dist.ReserveNode(job1)

	if strings.Compare(node1.Id(), nodes[0].Id()) != 0 {
		t.Error("Unexpected Node Returned", node1.Id(), nodes[0].Id())
	}

	select {
	case <-dist.freeCh:
		t.Error("Expected cluster to be full schedlued")
	default:
	}

	updateCh <- cm.NodeUpdate{
		Node:       nodes[1],
		UpdateType: cm.NodeAdded,
	}

	job2 := sched.Job{
		Id: "job2",
	}
	node2 := dist.ReserveNode(job2)

	if strings.Compare(node2.Id(), nodes[1].Id()) != 0 {
		t.Error("Unexpected Node Returned", node2.Id(), nodes[1].Id())
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

	clusterMock := cm.NewMockDynamicCluster(mockCtrl)
	clusterMock.EXPECT().Members().Return(nodes)
	clusterMock.EXPECT().NodeUpdates().Return(updateCh)

	dist := NewDynamicPoolDistributor(clusterMock)

	updateCh <- cm.NodeUpdate{
		Node:       nodes[0],
		UpdateType: cm.NodeRemoved,
	}

	job1 := sched.Job{
		Id: "job1",
	}
	node1 := dist.ReserveNode(job1)

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

	clusterMock := cm.NewMockDynamicCluster(mockCtrl)
	clusterMock.EXPECT().Members().Return(nodes)
	clusterMock.EXPECT().NodeUpdates().Return(updateCh)

	dist := NewDynamicPoolDistributor(clusterMock)

	// fully schedule the cluster
	job1 := sched.Job{
		Id: "job1",
	}
	node1 := dist.ReserveNode(job1)

	job2 := sched.Job{
		Id: "job2",
	}
	node2 := dist.ReserveNode(job2)

	// remove node0 from cluster
	updateCh <- cm.NodeUpdate{
		Node:       nodes[0],
		UpdateType: cm.NodeRemoved,
	}

	// return node1 & node2, node1 should get removed
	dist.ReleaseNode(node1)
	dist.ReleaseNode(node2)

	job3 := sched.Job{
		Id: "job3",
	}
	node3 := dist.ReserveNode(job3)

	if strings.Compare(node3.Id(), nodes[1].Id()) != 0 {
		t.Error("Unexpected Node Returned", node3.Id(), nodes[1].Id())
	}

	select {
	case <-dist.freeCh:
		t.Error("Expected cluster to be full schedlued")
	default:
	}
}
