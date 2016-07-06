package cluster_membership

//go:generate mockgen -source=cluster.go -package=cluster_membership -destination=cluster_mock.go

//
// Static Cluster Interface
//
type Cluster interface {
	//
	// Returns a Slice of Node in the Cluster
	//
	Members() []Node
}

//
// Implementations of a Dynamic Cluster
// Must implement both interfaces
//
type DynamicCluster interface {
	DynamicClusterState
	UpdatableCluster
}

//
// Interface which provides access to
// the state of a dynamic cluster
//
type DynamicClusterState interface {
	Cluster

	// Returns a Channel which contains Node Updates
	NodeUpdates() <-chan NodeUpdate
}

//
// Interface which allows a Dynamic Cluster
// to be updated
//
type UpdatableCluster interface {
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
