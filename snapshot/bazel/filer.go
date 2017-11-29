package bazel

import (
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
	"os/exec"
)

func MakeBzFiler() *bzFiler {
	return &bzFiler{
		updater: snapshots.MakeNoopUpdater(),
	}
}

// Satisfies snapshot.Checkouter, snapshot.Ingester, and snapshot.Updater
type bzFiler struct {
	// we use existing noopUpdater for snapshot.Updater
	updater snapshot.Updater
}

func (bf *bzFiler) RunCmd(args []string) ([]byte, error) {
	cmd := exec.Command("fs_util", args...)
	return cmd.Output()
}
