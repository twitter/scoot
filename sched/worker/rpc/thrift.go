package rpc

import (
	cm "github.com/scootdev/scoot/sched/clustermembership"
)

type thriftWorker struct {
	node cm.Node
}

func NewThriftWorker(node cm.Node) Worker {
	return &thriftWorker{node}
}

func (w *thriftWorker) Run(cmd Command) {
	return thrift.sendMessage(node, thrift.NewRunCommand(cmd))
}
