package cluster_membership

//go:generate mockgen -source=node.go -package=cluster_membership -destination=node_mock.go

import (
	"github.com/scootdev/scoot/sched"
)

type Node interface {
	Id() string
	SendMessage(task sched.TaskDefinition) error
}
