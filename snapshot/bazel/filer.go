package bazel

import (
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

func MakeBzFiler() *bzFiler {
	return &bzFiler{
		command: &bzCommand{},
		updater: snapshots.MakeNoopUpdater(),
	}
}

// Options are a variadic list of functions that take a *bzCommand as an arg
// and modify *bCommand's fields, e.g.
// localStorePath := func(bc *bzCommand) {
//     bc.localStorePath = "/path/to/local/store"
// }
func MakeBzFilerWithOptions(options ...func(*bzCommand)) *bzFiler {
	bf := &bzFiler{updater: snapshots.MakeNoopUpdater()}
	bc := bzCommand{}
	for _, opt := range options {
		opt(&bc)
	}
	bf.command = &bc
	return bf
}

// Satisfies snapshot.Checkouter, snapshot.Ingester, and snapshot.Updater
// Default command is fs_util, a tool provided by github.com/pantsbuild/pants which
// handles underlying implementation of bazel snapshot functionality
type bzFiler struct {
	command bzRunner
	updater snapshot.Updater
}
