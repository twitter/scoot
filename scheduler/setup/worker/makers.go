package worker

import (
	"time"

	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

// Makes a worker suitable for using as an in-memory worker.
func MakeInmemoryWorker(node cluster.Node) runner.Service {
	return MakeDoneWorker()
}

// Makes a worker suitable for using as an in-memory worker.
func MakeDoneWorker() runner.Service {
	ex := execers.NewDoneExecer()
	filerMap := runner.MakeRunTypeMap()
	filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: snapshots.MakeInvalidFiler(), IDC: nil}
	r := runners.NewSingleRunner(ex, filerMap, runners.NewNullOutputCreator(), nil, stats.NopDirsMonitor, runner.EmptyID, []func() error{}, []func() error{}, nil)
	chaos := runners.NewChaosRunner(r)
	chaos.SetDelay(time.Duration(50) * time.Millisecond)
	return chaos
}

// Makes a worker that uses a SimExecer. This is suitable for testing.
func MakeSimWorker() runner.Service {
	ex := execers.NewSimExecer()
	filerMap := runner.MakeRunTypeMap()
	filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: snapshots.MakeInvalidFiler(), IDC: nil}
	return runners.NewSingleRunner(ex, filerMap, runners.NewNullOutputCreator(), nil, stats.NopDirsMonitor, runner.EmptyID, []func() error{}, []func() error{}, nil)
}
