package workers

import (
	"time"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

// Makes a worker suitable for using as an in-memory worker.
func MakeInmemoryWorker(node cluster.Node, tmp *temp.TempDir) runner.Service {
	return MakeDoneWorker(tmp)
}

// Makes a worker suitable for using as an in-memory worker.
func MakeDoneWorker(tmp *temp.TempDir) runner.Service {
	ex := execers.NewDoneExecer()
	r := runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), nil, nil, runners.NewNullOutputCreator(), tmp, nil)
	chaos := runners.NewChaosRunner(r)
	chaos.SetDelay(time.Duration(50) * time.Millisecond)
	return chaos
}

// Makes a worker that uses a SimExecer. This is suitable for testing.
func MakeSimWorker(tmp *temp.TempDir) runner.Service {
	ex := execers.NewSimExecer()
	return runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), nil, nil, runners.NewNullOutputCreator(), tmp, nil)
}
