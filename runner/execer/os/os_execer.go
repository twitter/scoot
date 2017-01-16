package os

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
)

func NewExecer() execer.Execer {
	return &osExecer{}
}

type osExecer struct{}

type WriterDelegater interface {
	// Return an underlying Writer. Why? Because some methods type assert to
	// a more specific type and are more clever (e.g., if it's an *os.File, hook it up
	// directly to a new process's stdout/stderr.)
	// We care about this cleverness, so Output both is-a and has-a Writer
	// Cf. runner/runners/local_output.go
	WriterDelegate() io.Writer
}

func (e *osExecer) Exec(command execer.Command) (result execer.Process, err error) {
	if len(command.Argv) == 0 {
		return nil, errors.New("No command specified.")
	}

	cmd := exec.Command(command.Argv[0], command.Argv[1:]...)

	cmd.Stdout, cmd.Stderr, cmd.Dir = command.Stdout, command.Stderr, command.Dir
	// Make sure to get the best possible Writer, so if possible os/exec can connect
	// the command's stdout/stderr directly to a file, instead of having to go through
	// our delegation
	if stdoutW, ok := cmd.Stdout.(WriterDelegater); ok {
		cmd.Stdout = stdoutW.WriterDelegate()
	}
	if stderrW, ok := cmd.Stderr.(WriterDelegater); ok {
		cmd.Stderr = stderrW.WriterDelegate()
	}

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

	_, err = p.cmd.Process.Wait()
	if err, ok := err.(*exec.ExitError); ok {
		if status, ok := err.Sys().(syscall.WaitStatus); ok {
			result.ExitCode = status.ExitStatus()
		}
	}
	return result
}

func (p *osProcess) MemUsage() (runner.Memory, error) {
	if pgid, err := syscall.Getpgid(p.cmd.Process.Pid); err != nil {
		return 0, err
	} else {
		return memUsage(pgid)
	}
}

func memUsage(pgid int) (runner.Memory, error) {
	// Pass children of pgid from 'pgrep', and pgid itself, into 'ps' to get rss memory usages in KB, then sum them.
	// Note: there may be better ways to do this if we choose to handle osx/linux separately.
	str := "echo $(ps -orss= -p$(echo -n '%d,'; pgrep -P %d | tr '\n' ',') | tr '\n' '+') 0 | bc"
	cmd := exec.Command("bash", "-c", fmt.Sprintf(str, pgid, pgid))
	if usageKB, err := cmd.Output(); err != nil {
		return 0, err
	} else {
		u, err := strconv.Atoi(strings.Trim(string(usageKB), "\n"))
		return runner.Memory(u * 1024), err
	}
}
