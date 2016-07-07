package os

import (
	"bytes"
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
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	return &osProcess{cmd}, nil
}

type osProcess struct {
	cmd *exec.Cmd
}

func (p *osProcess) Wait() (result execer.ProcessStatus) {
	err := p.cmd.Wait()
	if err == nil {
		result.State = execer.COMPLETE
		result.ExitCode = 0
		// TODO(dbentley): set stdout and stderr
		return result
	}
	if err, ok := err.(*exec.ExitError); ok {
		if status, ok := err.Sys().(syscall.WaitStatus); ok {
			result.State = execer.COMPLETE
			result.ExitCode = status.ExitStatus()
			// TODO(dbentley): set stdout and stderr
			return result
		}
		result.State = execer.FAILED
		result.Error = "Could not find WaitStatus from exiterr.Sys()"
		return result
	}

	result.State = execer.FAILED
	result.Error = err.Error()
	return result
}
