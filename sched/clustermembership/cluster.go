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
// Interface which provides access to
// the state of a dynamic cluster
//
type DynamicClusterState struct {
	InitialMembers []Node
	Updates        <-chan NodeUpdate
}

//
// Interface which allows a Dynamic Cluster
// to be updated
//
// Implementations of this interface should provide
// A Factory method, that returns an instance of a
// DynamicCluster and a ClusterState object
//
type DynamicCluster interface {
	Cluster
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
