package scheduler

import (
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/queue"
)

// An update to our Scheduler's view of the world
type update interface {
	apply(s *schedulerState)
}

// An action represents work to do. This involves three steps:
// First, update our state to signal that the work has started.
// Second, fire off rpcs to talk to other systems.
// Third, each RPC returns an update when it's done.
type action interface {
	update
	// rpcs() []rpc
}

type rpc interface{}

type sagaRpc struct {
	sagaId string
	msg    saga.SagaMsg
}

type queueRpc struct {
	jobId string
	// No func because the only thing we can do is dequeue it
}

type workerRpc struct {
	workerId string
}
