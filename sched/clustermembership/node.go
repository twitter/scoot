package cluster_membership

import (
	msg "github.com/scootdev/scoot/messages"
)

type Node interface {
	Id() string
	SendMessage(task msg.Task) error
}
