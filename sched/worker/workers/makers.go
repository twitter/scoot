package workers

import (
	"time"

	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/snapshot/snapshots"
)

// Makes a worker suitable for using as an in-memory worker.
func MakeInmemoryWorker(node cluster.Node, tmp *temp.TempDir) runner.Service {
	return MakeDoneWorker(tmp)
}

// Makes a worker suitable for using as an in-memory worker.
func MakeDoneWorker(tmp *temp.TempDir) runner.Service {
	ex := execers.NewDoneExecer()
	r := runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), nil, runners.NewNullOutputCreator(), tmp, nil)
	chaos := runners.NewChaosRunner(r)
	chaos.SetDelay(time.Duration(50) * time.Millisecond)
	return chaos
}

// Makes a worker that uses a SimExecer. This is suitable for testing.
func MakeSimWorker(tmp *temp.TempDir) runner.Service {
	ex := execers.NewSimExecer()
	return runners.NewSingleRunner(ex, snapshots.MakeInvalidFiler(), nil, runners.NewNullOutputCreator(), tmp, nil)
}
