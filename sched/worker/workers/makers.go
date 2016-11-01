package workers

import (
	"time"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

// Makes a worker suitable for using as an in-memory worker.
func MakeInmemoryWorker(node cluster.Node) worker.Worker {
	ex := execers.NewDoneExecer()
	r := local.NewSimpleRunner(ex, snapshots.MakeInvalidCheckouter(), runners.NewNullOutputCreator())
	chaos := runners.NewChaosRunner(r)
	chaos.SetDelay(time.Duration(500) * time.Millisecond)
	return NewPollingWorker(chaos, time.Duration(250)*time.Millisecond)
}

// Makes a worker that uses a SimExecer. This is suitable for testing.
func MakeSimWorker() worker.Worker {
	ex := execers.NewSimExecer()
	r := local.NewSimpleRunner(ex, snapshots.MakeInvalidCheckouter(), runners.NewNullOutputCreator())
	return NewPollingWorker(r, time.Duration(10)*time.Microsecond)
}
