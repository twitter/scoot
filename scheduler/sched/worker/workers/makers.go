package workers

import (
	"time"

	"github.com/twitter/scoot/apiserver/snapshot"
	"github.com/twitter/scoot/apiserver/snapshot/snapshots"
	"github.com/twitter/scoot/common/cloud/cluster"
	"github.com/twitter/scoot/common/os/temp"
	"github.com/twitter/scoot/worker/runner"
	"github.com/twitter/scoot/worker/runner/execer/execers"
	"github.com/twitter/scoot/worker/runner/runners"
)

// Makes a worker suitable for using as an in-memory worker.
func MakeInmemoryWorker(node cluster.Node, tmp *temp.TempDir) runner.Service {
	return MakeDoneWorker(tmp)
}

// Makes a worker suitable for using as an in-memory worker.
func MakeDoneWorker(tmp *temp.TempDir) runner.Service {
	ex := execers.NewDoneExecer()
	filerMap := runner.MakeRunTypeMap()
	filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: snapshots.MakeInvalidFiler(), IDC: nil}
	r := runners.NewSingleRunner(ex, filerMap, runners.NewNullOutputCreator(), tmp, nil, runner.EmptyID)
	chaos := runners.NewChaosRunner(r)
	chaos.SetDelay(time.Duration(50) * time.Millisecond)
	return chaos
}

// Makes a worker that uses a SimExecer. This is suitable for testing.
func MakeSimWorker(tmp *temp.TempDir) runner.Service {
	ex := execers.NewSimExecer()
	filerMap := runner.MakeRunTypeMap()
	filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: snapshots.MakeInvalidFiler(), IDC: nil}
	return runners.NewSingleRunner(ex, filerMap, runners.NewNullOutputCreator(), tmp, nil, runner.EmptyID)
}
