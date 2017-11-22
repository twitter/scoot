package bazel

import (
	"github.com/twitter/scoot/snapshot/snapshots"
	"os/exec"
)

func MakeBzFiler() *bzFiler {
	return &bzFiler{
		checkouter: MakeBzCheckouter(),
		ingester:   MakeBzIngester(),
		updater:    snapshots.MakeNoopUpdater(),
	}
}

type bzFiler struct {
	checkouter *bzCheckouter
	ingester   *bzIngester
	updater    *snapshots.NoopUpdater
}

func (bf *bzFiler) runFsUtilCmd(args []string) error {
	cmd := exec.Command("fs_util", args...)
	return cmd.Run()
}

type bzCheckouter struct{}

type bzIngester struct{}

// type bzUpdater doesn't exist, we use existing noopUpdater

// Satisfies snapshot.Checkout interface
type bzCheckout struct{}

func MakeBzCheckouter() *bzCheckouter {
	return &bzCheckouter{}
}

func MakeBzIngester() *bzIngester {
	return &bzIngester{}
}
