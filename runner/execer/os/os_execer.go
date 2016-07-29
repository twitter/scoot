package os

import (
	"bytes"
	"errors"
	"os/exec"
	"syscall"

	"github.com/scootdev/scoot/runner/execer"
)

func NewExecer() execer.Execer {
	return &osExecer{}
}

type osExecer struct{}

func (e *osExecer) Exec(command execer.Command) (result execer.Process, err error) {
	if len(command.Argv) <= 0 {
		return nil, errors.New("No command specified.")
	}
	cmd := exec.Command(command.Argv[0], command.Argv[1:]...)
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdoutBuf, &stderrBuf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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

func (p *osProcess) Abort() (result execer.ProcessStatus) {
	result.State = execer.FAILED
	result.ExitCode = -1
	result.Error = "Aborted."
	pgid, err := syscall.Getpgid(p.cmd.Process.Pid)
	if err == nil {
		err = syscall.Kill(-pgid, 15)
	}
	if err != nil {
		result.Error = "Aborted. Parent only, couldn't kill by pgid."
	}
	err = p.cmd.Process.Kill()
	if err != nil {
		result.Error = "Aborted. Couldn't kill pgid or parent."
	}

	err = p.cmd.Wait()
	if err, ok := err.(*exec.ExitError); ok {
		if status, ok := err.Sys().(syscall.WaitStatus); ok {
			result.ExitCode = status.ExitStatus()
		}
	}
	return result
}
