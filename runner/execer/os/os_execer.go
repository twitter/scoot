package os

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/runner/execer"

	log "github.com/Sirupsen/logrus"
)

func NewExecer() execer.Execer {
	return &osExecer{}
}

// For now memory can be capped on a per-execer basis rather than a per-command basis.
// This is ok since we currently (Q1 2017) only support one run at a time in our codebase.
func NewBoundedExecer(memCap execer.Memory, stat stats.StatsReceiver) execer.Execer {
	return &osExecer{memCap: memCap, stat: stat.Scope("osexecer")}
}

type osExecer struct {
	// Best effort monitoring of command to kill it if resident memory usage exceeds this cap. Ignored if zero.
	memCap execer.Memory
	stat   stats.StatsReceiver
}

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

	proc := &osProcess{cmd: cmd}
	if e.memCap > 0 {
		go proc.monitorMem(e.memCap, e.stat)
	}
	return proc, nil
}

type osProcess struct {
	cmd    *exec.Cmd
	result *execer.ProcessStatus
	mutex  sync.Mutex
}

// Periodically check to make sure memory constraints are respected.
func (p *osProcess) monitorMem(memCap execer.Memory, stat stats.StatsReceiver) {
	memTicker := time.NewTicker(100 * time.Millisecond)
	defer memTicker.Stop()
	for {
		select {
		case <-memTicker.C:
			p.mutex.Lock()
			if p.result != nil {
				p.mutex.Unlock()
				return
			}
			usage, _ := memUsage(p.cmd.Process.Pid)
			stat.Gauge("memory").Update(int64(usage))
			if usage >= memCap {
				msg := fmt.Sprintf("Cmd exceeded MemoryCap: %d > %d (%v)", usage, memCap, p.cmd.Args)
				log.Info(msg)
				p.result = &execer.ProcessStatus{
					State: execer.FAILED,
					Error: msg,
				}
				p.mutex.Unlock()
				return
			}
			p.mutex.Unlock()
		}
	}
}

func (p *osProcess) Wait() (result execer.ProcessStatus) {
	err := p.cmd.Wait()
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.result != nil {
		return *p.result
	} else {
		p.result = &result
	}
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
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.result != nil {
		return *p.result
	} else {
		p.result = &result
	}

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

func memUsage(pgid int) (execer.Memory, error) {
	// Pass children of pgid from 'pgrep', and pgid itself, into 'ps' to get rss memory usages in KB, then sum them.
	// Note: there may be better ways to do this if we choose to handle osx/linux separately.
	str := `function getp(){ cps=$(pgrep -P $1); for cp in $cps; do echo "$cp"; getp $cp; done }`
	str += `;export P=%d; ps -orss $(getp $P) $P | grep -v RSS | tr '\n' '+' | grep -o '.*[^+]' | bc`
	cmd := exec.Command("bash", "-c", fmt.Sprintf(str, pgid))
	if usageKB, err := cmd.Output(); err != nil {
		return 0, err
	} else {
		u, err := strconv.Atoi(strings.Trim(string(usageKB), "\n"))
		return execer.Memory(u * 1024), err
	}
}
