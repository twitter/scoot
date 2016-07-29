package cluster

import (
	"fmt"
	"io"
)

// NodeUpdate represents a change to the cluster
type NodeUpdate struct {
	UpdateType NodeUpdateType
	Id         NodeId
	Node       Node // Only set for adds
}

func (u *NodeUpdate) String() string {
	return fmt.Sprintf("%v %v %v", u.UpdateType, u.Id, u.Node)
}

// Cluster represents a cluster of Nodes.
type Cluster interface {
	// Members returns the current members, or an error if they can't be determined.
	Members() []Node
	// Subscribe subscribes to changes to the cluster.
	Subscribe() Subscription
	// Stop monitoring this cluster
	Close() error
}

// Subscription is a subscription to cluster changes.
type Subscription struct {
	InitialMembers []Node            // The members at the time the subscription started
	Updates        chan []NodeUpdate // Updates as they happen
	Closer         io.Closer         // How to stop subscribing
}

// Helper functions to create NodeUpdates

func NewAdd(node Node) NodeUpdate {
	return NodeUpdate{
		NodeAdded,
		node.Id(),
		node,
	}
}

func NewRemove(id NodeId) NodeUpdate {
	return NodeUpdate{
		UpdateType: NodeRemoved,
		Id:         id,
	}
}
