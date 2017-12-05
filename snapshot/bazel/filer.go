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

func MakeBzFilerWithLocalStore(localStorePath string) *bzFiler {
	return &bzFiler{
		command:        "fs_util",
		updater:        snapshots.MakeNoopUpdater(),
		localStorePath: localStorePath,
	}
}

// Satisfies snapshot.Checkouter, snapshot.Ingester, and snapshot.Updater
type bzFiler struct {
	command        string
	updater        snapshot.Updater
	localStorePath string
	// Not yet implemented:
	// bypassLocalStore bool
	// skipServer       bool
	// serverAddress    string
}

func (bf *bzFiler) RunCmd(args []string) ([]byte, error) {
	if bf.localStorePath != "" {
		args = append(args, "--local_store_path", bf.localStorePath)
	}
	cmd := exec.Command(bf.command, args...)
	return cmd.Output()
}
