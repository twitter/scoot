package bazel

import (
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

func MakeDefaultBzFiler() *bzFiler {
	return &bzFiler{
		command: bzCommand{},
		updater: snapshots.MakeNoopUpdater(),
	}
}

func MakeBzFilerWithLocalStore(localStorePath string) *bzFiler {
	return &bzFiler{
		command: bzCommand{
			localStorePath: localStorePath,
		},
		updater: snapshots.MakeNoopUpdater(),
	}
}

// Satisfies snapshot.Checkouter, snapshot.Ingester, and snapshot.Updater
// Default command is fs_util, a tool provided by github.com/pantsbuild/pants which
// handles underlying implementation of bazel snapshot functionality
type bzFiler struct {
	command bzRunner
	updater snapshot.Updater
}
