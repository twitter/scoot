package cluster

import (
	"fmt"
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

type NodeUpdateSorter []NodeUpdate
func (n NodeUpdateSorter) Len() int 		  {return len(n)}
func (n NodeUpdateSorter) Swap(i, j int) 	  {n[i], n[j] = n[j], n[i]}
func (n NodeUpdateSorter) Less(i, j int) bool {return n[i].Id < n[j].Id}