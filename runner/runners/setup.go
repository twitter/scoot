package runners

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/wisechengyi/scoot/common/stats"
	"github.com/wisechengyi/scoot/ice"
	"github.com/wisechengyi/scoot/runner"
	"github.com/wisechengyi/scoot/runner/execer"
	"github.com/wisechengyi/scoot/runner/execer/execers"
	osexec "github.com/wisechengyi/scoot/runner/execer/os"
)

// Module returns a module that creates a new Runner.
func Module() ice.Module {
	return module{}
}

type module struct{}

// Install installs functions for creating a new Runner.
func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func(m execer.Memory, getMemFunc func() (int64, error), s stats.StatsReceiver) execer.Execer {
			return execers.MakeSimExecerInterceptor(execers.NewSimExecer(), osexec.NewBoundedExecer(m, getMemFunc, s))
		},
		func() runner.RunnerID {
			// suitable local testing purposes, but a production implementation would supply a unique ID
			hostname, _ := os.Hostname()
			hostname = fmt.Sprintf("%s-%d", hostname, rand.Intn(10000))
			return runner.RunnerID{ID: hostname}
		},
		func() *stats.DirsMonitor {
			return stats.NopDirsMonitor
		},
		NewSingleRunner,
	)
}
