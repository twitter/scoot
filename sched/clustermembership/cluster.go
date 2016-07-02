package cluster_membership

//go:generate mockgen -source=cluster.go -package=cluster_membership -destination=cluster_mock.go

type Cluster interface {
	//
	// Returns a Slice of Node in the Cluster
	//
	Members() []Node
}

type DynamicCluster interface {
	Cluster

	// Returns a Channel which contains Node Updates
	NodeUpdates() <-chan NodeUpdate

	//
	// Adds A Node to the Cluster
	//
	AddNode(n Node)

	//
	// Permanently Removes A Node from the Cluster
	//
	RemoveNode(nodeId string)
}

type NodeUpdateType int

const (
	NodeAdded NodeUpdateType = iota
	NodeRemoved
)

type NodeUpdate struct {
	UpdateType NodeUpdateType
	Node       Node
}
