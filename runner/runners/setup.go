package runners

import (
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/execers"
	osexec "github.com/scootdev/scoot/runner/execer/os"
	"github.com/scootdev/scoot/snapshot"
)

// Module returns a module that creates a new Runner.
func Module() ice.Module {
	return module{}
}

type module struct{}

// Install installs functions for creating a new Runner.
func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func(m execer.Memory, s stats.StatsReceiver) execer.Execer {
			return execers.MakeSimExecerInterceptor(execers.NewSimExecer(), osexec.NewBoundedExecer(m, s))
		},
		func(db snapshot.DB) snapshot.Filer {
			return snapshot.NewDBAdapter(db)
		},
		NewSingleRunner,
	)
}
