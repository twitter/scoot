package cluster

import (
	"fmt"
)

type NodeId string

type Node interface {
	// A unique node identifier, like 'host:port'
	Id() NodeId
}

type idNode struct {
	id NodeId
}

func (n *idNode) String() string {
	return string(n.id)
}

func NewIdNode(id string) Node {
	return &idNode{NodeId(id)}
}

func NewIdNodes(num int) []Node {
	r := []Node{}
	for i := 0; i < num; i++ {
		r = append(r, NewIdNode(fmt.Sprintf("node%d", i+1)))
	}
	return r
}

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
