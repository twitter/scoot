package distributor

import (
	"github.com/golang/mock/gomock"
	"github.com/scootdev/scoot/sched"
	cm "github.com/scootdev/scoot/sched/clustermembership"
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
	node1 := dist.ReserveNode(job1)
	dist.ReleaseNode(node1)

	job2 := sched.Job{
		Id: "job2",
	}
	node2 := dist.ReserveNode(job2)

	if node1.Id() != node2.Id() {
		t.Error("Expected nodes to be the same Id since there is only one in the cluster")
	}
}
