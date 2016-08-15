package cluster

type NodeId string

type Node interface {
	// A unique node identifier, like 'host:port'
	Id() NodeId
}

type Nodes []Node

// Len is part of sort.Interface.
func (n Nodes) Len() int {
	return len(n)
}

// Swap is part of sort.Interface.
func (n Nodes) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

// Less is part of sort.Interface.
func (n Nodes) Less(i, j int) bool {
	return n[i].Id() < n[j].Id()
}

type NodeUpdateType int

const (
	NodeAdded NodeUpdateType = iota
	NodeRemoved
)
