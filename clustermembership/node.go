package cluster_membership

type Node interface {
	Id() string
	SendMessage(msg string) error
}
