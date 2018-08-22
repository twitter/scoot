package testhelpers

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func InstallBinary(name string) {
	cmd := exec.Command("go", "install", "./binaries/"+name)
	cmd.Run()
}

func GetGopath() (gopath string, err error) {
	gopath = os.Getenv("GOPATH")
	if gopath == "" {
		err = fmt.Errorf("GOPATH not set")
	}
	return strings.Split(gopath, ":")[0], err
}
