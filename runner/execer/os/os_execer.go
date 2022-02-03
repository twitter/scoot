package os

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	scooterror "github.com/twitter/scoot/common/errors"
	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner/execer"

	log "github.com/sirupsen/logrus"
)

type WriterDelegater interface {
	// Return an underlying Writer. Why? Because some methods type assert to
	// a more specific type and are more clever (e.g., if it's an *os.File, hook it up
	// directly to a new process's stdout/stderr.)
	// We care about this cleverness, so Output both is-a and has-a Writer
	// Cf. runner/runners/local_output.go
	WriterDelegate() io.Writer
}

// Implements runner/execer.Execer
type osExecer struct {
	// Best effort monitoring of command to kill it if resident memory usage exceeds this cap
	memCap execer.Memory
	stat   stats.StatsReceiver
	pw     processWatcher
}

// Implements runner/execer.Process
type osProcess struct {
	cmd     *exec.Cmd
	wg      *sync.WaitGroup
	waiting bool
	result  *execer.ProcessStatus
	mutex   sync.Mutex
	ats     int // Abort Timeout before sigkill, in Seconds
	tags.LogTags
}

type proc struct {
	pid  int
	pgid int
	ppid int
	rss  int
}

// NewBoundedExecer returns an execer with a ProcGetter and, if non-zero values are provided, a memCap and a StatsReceiver
func NewBoundedExecer(memCap execer.Memory, stat stats.StatsReceiver) *osExecer {
	oe := &osExecer{pw: NewOsProcWatcher()}
	if memCap != 0 {
		oe.memCap = memCap
	}
	if stat != nil {
		oe.stat = stat
	}
	return oe
}

// Start a command, monitor its memory, and return an &osProcess wrapper for it
func (e *osExecer) Exec(command execer.Command) (execer.Process, error) {
	if len(command.Argv) == 0 {
		return nil, errors.New("No command specified.")
	}

	cmd := exec.Command(command.Argv[0], command.Argv[1:]...)
	cmd.Dir = command.Dir

	// Use the parent environment plus whatever additional env vars are provided.
	cmd.Env = os.Environ()
	for k, v := range command.EnvVars {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

	// Sets pgid of all child processes to cmd's pid
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Make sure to get the best possible Writer, so if possible os/exec can connect
	// the command's stdout/stderr directly to a file, instead of having to go through
	// our delegation
	if stdoutW, ok := command.Stdout.(WriterDelegater); ok {
		command.Stdout = stdoutW.WriterDelegate()
	}
	if stderrW, ok := cmd.Stderr.(WriterDelegater); ok {
		command.Stderr = stderrW.WriterDelegate()
	}

	// Use pipes due to possible hang in process.Wait().
	// See: https://github.com/noxiouz/stout/commit/42cc533a0bece540f2424faff2a960876b21ffd2
	stdErrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	stdOutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(command.Stderr, stdErrPipe)
	}()
	go func() {
		defer wg.Done()
		io.Copy(command.Stdout, stdOutPipe)
	}()

	// Async start of the command.
	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	proc := &osProcess{cmd: cmd, wg: &wg, ats: AbortTimeoutSec, LogTags: command.LogTags}
	if e.memCap > 0 {
		go e.monitorMem(proc, command.MemCh)
	}

	return proc, nil
}

// Periodically check to make sure memory constraints are respected,
// and clean up after ourselves when the process has completed
func (e *osExecer) monitorMem(p *osProcess, memCh chan execer.ProcessStatus) {
	pid := p.cmd.Process.Pid
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		log.WithFields(
			log.Fields{
				"pid":    pid,
				"error":  err,
				"tag":    p.Tag,
				"jobID":  p.JobID,
				"taskID": p.TaskID,
			}).Error("Error finding pgid")
	} else {
		defer cleanupProcs(pgid)
	}
	thresholdsIdx := 0
	reportThresholds := []float64{0, .25, .5, .75, .85, .9, .93, .95, .96, .97, .98, .99, 1}
	memTicker := time.NewTicker(250 * time.Millisecond)
	defer memTicker.Stop()
	log.WithFields(
		log.Fields{
			"pid":    pid,
			"tag":    p.Tag,
			"jobID":  p.JobID,
			"taskID": p.TaskID,
		}).Info("Monitoring memory")
	for {
		select {
		case <-memTicker.C:
			p.mutex.Lock()
			// Process is complete
			if p.result != nil {
				p.mutex.Unlock()
				log.WithFields(
					log.Fields{
						"pid":    pid,
						"tag":    p.Tag,
						"jobID":  p.JobID,
						"taskID": p.TaskID,
					}).Info("Finished monitoring memory")
				return
			}
			if err := e.pw.getAndSetProcs(); err != nil {
				log.Error(err)
			}
			mem, _ := e.pw.memUsage(pid)
			e.stat.Gauge(stats.WorkerMemory).Update(int64(mem))
			// Aborting process, above memCap
			if mem >= e.memCap {
				msg := fmt.Sprintf("Cmd exceeded MemoryCap, aborting %d: %d > %d (%v)", pid, mem, e.memCap, p.cmd.Args)
				log.WithFields(
					log.Fields{
						"mem":    mem,
						"memCap": e.memCap,
						"args":   p.cmd.Args,
						"pid":    pid,
						"tag":    p.Tag,
						"jobID":  p.JobID,
						"taskID": p.TaskID,
					}).Info(msg)
				p.result = &execer.ProcessStatus{
					State:    execer.COMPLETE,
					Error:    msg,
					ExitCode: 1,
				}
				if memCh != nil {
					memCh <- *p.result
				}
				p.mutex.Unlock()
				p.MemCapKill()
				return
			}
			// Report on larger changes when utilization is low, and smaller changes as utilization reaches 100%.
			memUsagePct := math.Min(1.0, float64(mem)/float64(e.memCap))
			if memUsagePct > reportThresholds[thresholdsIdx] {
				log.WithFields(
					log.Fields{
						"memUsagePct": int(memUsagePct * 100),
						"mem":         mem,
						"memCap":      e.memCap,
						"args":        p.cmd.Args,
						"pid":         pid,
						"tag":         p.Tag,
						"jobID":       p.JobID,
						"taskID":      p.TaskID,
					}).Infof("Increased mem_cap utilization for pid %d to %d", pid, int(memUsagePct*100))

				// Trace output with timeout since it seems CombinedOutput() sometimes fails to return.
				if log.IsLevelEnabled(log.TraceLevel) {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					ps, err := exec.CommandContext(ctx, "ps", "-u", os.Getenv("USER"), "-opid,sess,ppid,pgid,rss,args").CombinedOutput()
					log.WithFields(
						log.Fields{
							"pid":    pid,
							"ps":     string(ps),
							"err":    err,
							"errCtx": ctx.Err(),
							"tag":    p.Tag,
							"jobID":  p.JobID,
							"taskID": p.TaskID,
						}).Tracef("ps after increasing mem_cap utilization for pid %d", pid)
					cancel()
				}

				for memUsagePct > reportThresholds[thresholdsIdx] {
					thresholdsIdx++
				}
			}
			p.mutex.Unlock()
		}
	}
}

// Wait for the process to finish.
// If the command finishes without error return the status COMPLETE and exit Code 0.
// If the command fails, and we can get the exit code from the command, return COMPLETE with the failing exit code.
// if the command fails and we cannot get the exit code from the command, return FAILED and the error
// that prevented getting the exit code.
func (p *osProcess) Wait() (result execer.ProcessStatus) {
	p.mutex.Lock()
	p.waiting = true
	p.mutex.Unlock()

	// Wait for the output goroutines to finish then wait on the process itself to release resources.
	p.wg.Wait()
	pid := p.cmd.Process.Pid

	err := p.cmd.Wait()
	log.WithFields(
		log.Fields{
			"pid":    pid,
			"tag":    p.Tag,
			"jobID":  p.JobID,
			"taskID": p.TaskID,
		}).Infof("Finished waiting for process")

	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.waiting = false

	// Trace output with timeout since it seems CombinedOutput() sometimes fails to return.
	if log.IsLevelEnabled(log.TraceLevel) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ps, errDbg := exec.CommandContext(ctx, "ps", "-u", os.Getenv("USER"), "-opid,sess,ppid,pgid,rss,args").CombinedOutput()
		log.WithFields(
			log.Fields{
				"pid":    pid,
				"tag":    p.Tag,
				"jobID":  p.JobID,
				"taskID": p.TaskID,
				"ps":     string(ps),
				"err":    errDbg,
				"errCtx": ctx.Err(),
			}).Tracef("Current ps for pid %d", pid)
		cancel()
	}

	if p.result != nil {
		return *p.result
	} else {
		p.result = &result
	}
	if err == nil {
		// the command finished without an error
		result.State = execer.COMPLETE
		result.ExitCode = 0
		// stdout and stderr are collected and set by (invoke.go) runner
		return result
	}
	if err, ok := err.(*exec.ExitError); ok {
		// the command returned an error, if we can get a WaitStatus from the error,
		// we can get the commands exit code
		if status, ok := err.Sys().(syscall.WaitStatus); ok {
			result.State = execer.COMPLETE
			result.ExitCode = scooterror.ExitCode(status.ExitStatus())
			// stdout and stderr are collected and set by (invoke.go) runner
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

// Attempt to SIGTERM process, allowing for graceful exit
// SIGKILL after 10 seconds or if osProcess.cmd.Wait() returns an error
func (p *osProcess) Abort() execer.ProcessStatus {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.result != nil {
		return *p.result
	} else {
		p.result = &execer.ProcessStatus{}
	}
	p.result.State = execer.FAILED
	p.result.ExitCode = -1
	p.result.Error = "Aborted"

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		msg := fmt.Sprintf("Error aborting command via SIGTERM: %s.", err)
		log.WithFields(
			log.Fields{
				"pid":    p.cmd.Process.Pid,
				"tag":    p.Tag,
				"jobID":  p.JobID,
				"taskID": p.TaskID,
			}).Errorf(msg)
		p.KillAndWait(msg)
	} else {
		log.WithFields(
			log.Fields{
				"pid":    p.cmd.Process.Pid,
				"tag":    p.Tag,
				"jobID":  p.JobID,
				"taskID": p.TaskID,
			}).Info("Aborting process via SIGTERM")
	}

	// Add buffer in case of race condition where both <-cmdDoneCh returns an error & timeout is exceeded at same time
	errCh := make(chan error, 1)
	go func() {
		select {
		case <-time.After(time.Second * time.Duration(p.ats)):
			errCh <- errors.New(fmt.Sprintf("%d second timeout exceeded.", p.ats))
		}
	}()

	cmdDoneCh := make(chan error)

	// Wait in the process if nothing already has claimed it;
	// if cmd.Wait() was already called, calling it again is an immediate error.
	// If already called, just poll periodically if the process has exited
	if !p.waiting {
		go func() {
			// p.wg ignored
			cmdDoneCh <- p.cmd.Wait()
		}()
	} else {
		go func() {
			timeout := time.Now().Add(time.Second * time.Duration(p.ats))
			for time.Now().Before(timeout) {
				// note that we can't rely on ProcessState.Exited() - not true when p is signaled
				if p.cmd.ProcessState != nil {
					cmdDoneCh <- nil
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	for {
		select {
		case err := <-cmdDoneCh:
			sigtermed := false
			if err == nil {
				sigtermed = true
			}
			if err != nil {
				if err, ok := err.(*exec.ExitError); ok {
					if status, ok := err.Sys().(syscall.WaitStatus); ok {
						if status.Signaled() {
							sigtermed = true
						}
					}
				}
			}

			if sigtermed {
				log.WithFields(
					log.Fields{
						"pid":    p.cmd.Process.Pid,
						"tag":    p.Tag,
						"jobID":  p.JobID,
						"taskID": p.TaskID,
					}).Info("Command finished via SIGTERM")
				p.result.Error += " (SIGTERM)"
				return *p.result
			} else {
				// We weren't able to infer the task exited either normally or due to sigterm
				msg := fmt.Sprintf("Command failed to terminate successfully: %v", err)
				log.WithFields(
					log.Fields{
						"pid":    p.cmd.Process.Pid,
						"tag":    p.Tag,
						"jobID":  p.JobID,
						"taskID": p.TaskID,
					}).Error(msg)
				errCh <- errors.New(msg)
				// Loop back and pull from errCh to force cleanup
			}
		case msg := <-errCh:
			log.WithFields(
				log.Fields{
					"pid":    p.cmd.Process.Pid,
					"tag":    p.Tag,
					"jobID":  p.JobID,
					"taskID": p.TaskID,
				}).Error(msg)
			p.KillAndWait(fmt.Sprintf("%s. Killing command.", msg))
			p.result.Error += " (SIGKILL)"
			return *p.result
		default:
		}
	}
}

// Kill osProcess for exceeding MemCap
func (p *osProcess) MemCapKill() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.result == nil {
		p.result = &execer.ProcessStatus{}
	}
	p.result.State = execer.FAILED
	p.result.ExitCode = -1
	p.KillAndWait("Killed for memory usage over MemCap")
}

// Kills osProcess via SIGKILL and all processes of its pgid
func (p *osProcess) KillAndWait(resultError string) {
	pgid, err := syscall.Getpgid(p.cmd.Process.Pid)
	if err != nil {
		log.WithFields(
			log.Fields{
				"pid":    p.cmd.Process.Pid,
				"error":  err,
				"tag":    p.Tag,
				"jobID":  p.JobID,
				"taskID": p.TaskID,
			}).Error("Error finding pgid")
	} else {
		defer cleanupProcs(pgid)
	}
	p.result.Error += fmt.Sprintf(" %s", resultError)
	err = p.cmd.Process.Kill()
	if err != nil {
		p.result.Error += fmt.Sprintf(" Couldn't kill process: %s. Will still attempt cleanup.", err)
	}
	_, err = p.cmd.Process.Wait()
	if err, ok := err.(*exec.ExitError); ok {
		if status, ok := err.Sys().(syscall.WaitStatus); ok {
			p.result.ExitCode = scooterror.ExitCode(status.ExitStatus())
		}
	}
}

// Kill process along with all child processes, assuming no child processes called setpgid
func cleanupProcs(pgid int) (err error) {
	log.WithFields(
		log.Fields{
			"pgid": pgid,
		}).Info("Cleaning up pgid")
	if err = syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
		log.WithFields(
			log.Fields{
				"pgid":  pgid,
				"error": err,
			}).Error("Error cleaning up pgid")
	}
	return err
}
