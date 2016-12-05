// Package worker provides the main Worker inferface that the scheduler uses to
// run Tasks on worker nodes. This also includes a WorkerFactory which allows
// for access of a Worker from a cluster Node.
package worker

//go:generate mockgen -source=worker.go -package=worker -destination=worker_mock.go

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched"
)

// Create a Worker controller that talks to node
type WorkerFactory func(node cluster.Node) Worker

// Worker gives the Scheduler a generic way to complete work.
type Worker interface {
	Start(task sched.TaskDefinition) (runner.ProcessStatus, error)
	Status(runId runner.RunId) (runner.ProcessStatus, error)
	Wait(runId runner.RunId) (runner.ProcessStatus, error)
	RunAndWait(task sched.TaskDefinition) (runner.ProcessStatus, error)
}

// We suspect we will only have 1 implementation of this interface for a long time: workers.PollingWorker
// Eventually, we may want the worker to push results to the scheduler instead of
// having to pull.
