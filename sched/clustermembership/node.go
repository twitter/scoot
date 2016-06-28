package cluster_membership

import (
	"github.com/scootdev/scoot/sched"
)

type Node interface {
	Id() string
	SendMessage(task sched.Task) error
}
