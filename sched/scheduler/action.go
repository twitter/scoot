package scheduler

import (
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/worker"
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

type dequeueWorkItem struct {
	jobId string
}

type workerRpc struct {
	workerId string
	call     func(w worker.Worker) workerReply
}

func (r logSagaMsg) rpc()      {}
func (r dequeueWorkItem) rpc() {}
func (r workerRpc) rpc()       {}

type errorReply struct {
	err error
}

type workerReply func(*schedulerState)

func (r errorReply) reply()  {}
func (r workerReply) reply() {}
