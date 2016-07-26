package cluster

type NodeId string

type Node interface {
	Id() NodeId
}

type NodeUpdateType int

const (
	NodeAdded NodeUpdateType = iota
	NodeRemoved
)
