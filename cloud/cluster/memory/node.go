package memory

import (
	"fmt"
	"github.com/scootdev/scoot/cloud/cluster"
)

type idNode struct {
	id cluster.NodeId
}

func (n *idNode) Id() cluster.NodeId {
	return n.id
}

func (n *idNode) String() string {
	return string(n.id)
}

func NewIdNode(id string) cluster.Node {
	return &idNode{cluster.NodeId(id)}
}

var _ cluster.Node = (*idNode)(nil)

func NewIdNodes(num int) []cluster.Node {
	r := []cluster.Node{}
	for i := 0; i < num; i++ {
		r = append(r, NewIdNode(fmt.Sprintf("node%d", i+1)))
	}
	return r
}
