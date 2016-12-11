package cluster

import (
	"fmt"
)

// A unique node identifier, like 'host:port'
type NodeId string

// This defines what can represent a Scoot node in a cluster
type Node interface {
	// A unique node identifier, like 'host:thriftPort'
	Id() NodeId
}

type idNode struct {
	id NodeId
}

func (n *idNode) String() string {
	return string(n.id)
}

func NewIdNode(id string) Node {
	return &idNode{id: NodeId(id)}
}

func NewIdNodes(num int) []Node {
	r := []Node{}
	for i := 0; i < num; i++ {
		r = append(r, NewIdNode(fmt.Sprintf("node%d", i+1)))
	}
	return r
}

// Implements interface Node.Id()
func (n *idNode) Id() NodeId {
	return n.id
}

type NodeSorter []Node

func (n NodeSorter) Len() int           { return len(n) }
func (n NodeSorter) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n NodeSorter) Less(i, j int) bool { return n[i].Id() < n[j].Id() }

type NodeUpdateType int

const (
	NodeAdded NodeUpdateType = iota
	NodeRemoved
)

var _ Node = (*idNode)(nil)

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
