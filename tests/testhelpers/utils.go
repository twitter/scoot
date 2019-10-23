package testhelpers

import "os/exec"

func InstallBinaries() error {
	cmd := exec.Command("go", "install", "./...")
	return cmd.Run()
}
