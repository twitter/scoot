package testhelpers

import "os/exec"

func InstallBinaries() {
	cmd := exec.Command("go", "install", "./...")
	cmd.Run()
}
