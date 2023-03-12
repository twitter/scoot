package os

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	scooterror "github.com/wisechengyi/scoot/common/errors"
	"github.com/wisechengyi/scoot/common/log/tags"
	scootexecer "github.com/wisechengyi/scoot/runner/execer"

	log "github.com/sirupsen/logrus"
)

// Implements runner/scootexecer.Process
type process struct {
	cmd     *exec.Cmd
	wg      *sync.WaitGroup
	waiting bool
	result  *scootexecer.ProcessStatus
	mutex   sync.Mutex
	ats     int // Abort Timeout before sigkill, in Seconds
	tags.LogTags
}

// Wait for the process to finish.
// If the command finishes without error return the status COMPLETE and exit Code 0.
// If the command fails, and we can get the exit code from the command, return COMPLETE with the failing exit code.
// if the command fails and we cannot get the exit code from the command, return FAILED and the error
// that prevented getting the exit code.
func (p *process) Wait() (result scootexecer.ProcessStatus) {
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
		result.State = scootexecer.COMPLETE
		result.ExitCode = 0
		// stdout and stderr are collected and set by (invoke.go) runner
		return result
	}
	if err, ok := err.(*exec.ExitError); ok {
		// the command returned an error, if we can get a WaitStatus from the error,
		// we can get the commands exit code
		if status, ok := err.Sys().(syscall.WaitStatus); ok {
			result.State = scootexecer.COMPLETE
			result.ExitCode = scooterror.ExitCode(status.ExitStatus())
			// stdout and stderr are collected and set by (invoke.go) runner
			return result
		}
		result.State = scootexecer.FAILED
		result.Error = "Could not find WaitStatus from exiterr.Sys()"
		return result
	}

	result.State = scootexecer.FAILED
	result.Error = err.Error()
	return result
}

// Attempt to SIGTERM process, allowing for graceful exit
// SIGKILL after 10 seconds or if process.cmd.Wait() returns an error
func (p *process) Abort() scootexecer.ProcessStatus {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.result != nil {
		return *p.result
	} else {
		p.result = &scootexecer.ProcessStatus{}
	}
	p.result.State = scootexecer.FAILED
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

// Kill process for exceeding MemCap
func (p *process) MemCapKill() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.result == nil {
		p.result = &scootexecer.ProcessStatus{}
	}
	p.result.State = scootexecer.FAILED
	p.result.ExitCode = -1
	p.KillAndWait("Killed for memory usage over MemCap")
}

// Kills process via SIGKILL and all processes of its pgid
func (p *process) KillAndWait(resultError string) {
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
