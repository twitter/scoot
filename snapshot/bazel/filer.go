package bazel

import (
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
	"os/exec"
)

func MakeDefaultBzFiler() *bzFiler {
	return &bzFiler{
		command: "fs_util",
		updater: snapshots.MakeNoopUpdater(),
	}
}

// Satisfies snapshot.Checkouter, snapshot.Ingester, and snapshot.Updater
type bzFiler struct {
	command string
	updater snapshot.Updater
}

func (bf *bzFiler) RunCmd(args []string) ([]byte, error) {
	cmd := exec.Command(bf.command, args...)
	return cmd.Output()
}
