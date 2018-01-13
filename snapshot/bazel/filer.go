package bazel

import (
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

func MakeBzFiler() *BzFiler {
	return &BzFiler{
		command: &bzCommand{},
		updater: snapshots.MakeNoopUpdater(),
	}
}

// Options is a variadic list of functions that take a *bzCommand as an arg
// and modify its fields, e.g.
// localStorePath := func(bc *bzCommand) {
//     bc.localStorePath = "/path/to/local/store"
// }
func MakeBzFilerWithOptions(options ...func(*bzCommand)) *BzFiler {
	return &BzFiler{
		updater: snapshots.MakeNoopUpdater(),
		command: MakeBzCommandWithOptions(options...),
	}
}

func MakeBzFilerWithOptionsKeepCheckouts(options ...func(*bzCommand)) *BzFiler {
	return &BzFiler{
		updater:       snapshots.MakeNoopUpdater(),
		command:       MakeBzCommandWithOptions(options...),
		keepCheckouts: true,
	}
}

// Satisfies snapshot.Checkouter, snapshot.Ingester, and snapshot.Updater
// Default command is fs_util, a tool provided by github.com/pantsbuild/pants which
// handles underlying implementation of bazel snapshot functionality
type BzFiler struct {
	command       bzRunner
	updater       snapshot.Updater
	keepCheckouts bool
	// keepCheckouts exists for debuggability. Instead of removing checkouts on release,
	// we can optionally keep them to inspect
}
