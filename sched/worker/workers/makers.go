package workers

import (
	"time"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

// Makes a worker suitable for using as an in-memory worker.
func MakeInmemoryWorker(node cluster.Node) worker.Worker {
	ex := execers.NewDoneExecer()
	r := runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), runners.NewNullOutputCreator())
	chaos := runners.NewChaosRunner(r)
	chaos.SetDelay(time.Duration(500) * time.Millisecond)
	return NewServiceWorker(chaos, time.Second)
}

// Makes a worker that uses a SimExecer. This is suitable for testing.
func MakeSimWorker() worker.Worker {
	ex := execers.NewSimExecer()
	r := runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), runners.NewNullOutputCreator())
	return NewServiceWorker(r, time.Second)
}
