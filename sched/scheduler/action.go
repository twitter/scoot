package scheduler

import (
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/workerapi"
)

// An action represents work to do. This involves three steps:
// First, update our state to signal that the work has started.
// Second, fire off rpcs to talk to other systems.
// Third, each RPC returns an update when it's done.
type action interface {
	apply(s *schedulerState) []rpc
}

type rpc interface {
	rpc()
}

// A reply from an RPC
type reply interface {
	reply()
}

type logSagaMsg struct {
	sagaId string
	msg    saga.Message
	final  bool
}

type respondWorkItem struct {
	jobId   string
	claimed bool
}

type workerRpc interface {
	rpc
	workerId() string
	call(w workerapi.Worker) workerReply
}

func (r logSagaMsg) rpc()      {}
func (r respondWorkItem) rpc() {}

type sagaLogReply struct {
	id  string
	err error
}

type queueReply struct {
	err error
}

type workerReply interface {
	reply
	apply(st *schedulerState)
}

func (r sagaLogReply) reply() {}
func (r queueReply) reply()   {}
