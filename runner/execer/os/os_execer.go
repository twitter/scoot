package os

import (
	"bytes"
	"fmt"
	"github.com/scootdev/scoot/runner/execer"
	"os/exec"
	"syscall"
)

func NewExecer() execer.Execer {
	return &osExecer{}
}

type osExecer struct{}

func (e *osExecer) Exec(command execer.Command) (result execer.Process, err error) {
	cmd := exec.Command(command.Argv[0], command.Argv[1:]...)
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdoutBuf, &stderrBuf
	err := cmd.Start()
	if err != nil {
		return result, err
	}
	err = cmd.Wait()
	if err != nil {
		if err, ok := err.(*exec.ExitError); ok {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				return
			}
		}
		return result, err
	}

	return result, fmt.Errorf("Fooled you!")
}

type osProcess struct{}
