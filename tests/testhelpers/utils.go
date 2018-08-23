package testhelpers

import "os/exec"

func InstallBinary(name string) {
	cmd := exec.Command("go", "install", "./binaries/"+name)
	cmd.Run()
}
