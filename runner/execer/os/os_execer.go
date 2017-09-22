package os

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner/execer"

	log "github.com/sirupsen/logrus"
)

func NewExecer() *osExecer {
	return &osExecer{}
}

// For now memory can be capped on a per-execer basis rather than a per-command basis.
// This is ok since we currently (Q1 2017) only support one run at a time in our codebase.
func NewBoundedExecer(memCap execer.Memory, stat stats.StatsReceiver) *osExecer {
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

	// Sets pgid of all child processes to cmd's pid
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	proc := &osProcess{cmd: cmd, LogTags: command.LogTags}
	if e.memCap > 0 {
		go e.monitorMem(proc)
	}
	return proc, nil
}

type osProcess struct {
	cmd    *exec.Cmd
	result *execer.ProcessStatus
	mutex  sync.Mutex
	tags.LogTags
}

// TODO(rcouto): More we can do here to make sure we're
// cleaning up after ourselves completely / not leaving
// orphaned processes behind
//
// Periodically check to make sure memory constraints are respected,
// and clean up after ourselves when the process has completed
func (e *osExecer) monitorMem(p *osProcess) {
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
			mem, _ := e.memUsage(pid)
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
					State: execer.FAILED,
					Error: msg,
				}
				p.mutex.Unlock()
				p.Abort()
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
				ps, err := exec.Command("ps", "-u", os.Getenv("USER"), "-opid,sess,ppid,pgid,rss,args").CombinedOutput()
				log.WithFields(
					log.Fields{
						"pid":    pid,
						"ps":     string(ps),
						"err":    err,
						"tag":    p.Tag,
						"jobID":  p.JobID,
						"taskID": p.TaskID,
					}).Debugf("ps after increasing mem_cap utilization for pid %d", pid)
				for memUsagePct > reportThresholds[thresholdsIdx] {
					thresholdsIdx++
				}
			}
			p.mutex.Unlock()
		}
	}
}

// Query for all sets of (pid, pgid, rss). Given a pid, find its associated pgid.
// From there, sum the memory of all processes with the same pgid.
func (e *osExecer) memUsage(pid int) (execer.Memory, error) {
	str := `
PID=%d
PSLIST=$(ps -e -o pid= -o pgid= -o rss= | tr '\n' ';' | sed 's,;$,,')
echo "

processes=dict()
memory=dict()
id=None
total=0
for line in \"$PSLIST\".split(';'):
  pid, pgid, mem = tuple(line.split())
  if pid == \"$PID\":
    id = pgid
  processes.setdefault(pgid, []).append(pid)
  memory[pid] = mem
for p in processes.setdefault(id, []):
  total += int(memory[p])
print total

" | python
`
	cmd := exec.Command("bash", "-c", fmt.Sprintf(str, pid))
	if usageKB, err := cmd.Output(); err != nil {
		return 0, err
	} else {
		u, err := strconv.Atoi(strings.TrimSpace(string(usageKB)))
		return execer.Memory(u * 1024), err
	}
}

/*
Wait for the process to finish.

If the command finishes without error return the status COMPLETE and exit Code 0.

If the command fails, and we can get the exit code from the command, return COMPLETE with the failing exit code.

if the command fails and we cannot get the exit code from the command, return FAILED and the error
that prevented getting the exit code.
*/
func (p *osProcess) Wait() (result execer.ProcessStatus) {
	pid := p.cmd.Process.Pid
	err := p.cmd.Wait()
	p.mutex.Lock()
	defer p.mutex.Unlock()
	ps, _ := exec.Command("ps", "-u", os.Getenv("USER"), "-opid,sess,ppid,pgid,rss,args").CombinedOutput()
	log.WithFields(
		log.Fields{
			"pid":    pid,
			"tag":    p.Tag,
			"jobID":  p.JobID,
			"taskID": p.TaskID,
			"ps":     string(ps),
		}).Debugf("Current ps for pid %d", pid)

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
			result.ExitCode = status.ExitStatus()
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

	err := p.cmd.Process.Kill()
	if err != nil {
		result.Error = "Aborted. Couldn't kill process. Will still attempt cleanup."
	}
	_, err = p.cmd.Process.Wait()
	if err, ok := err.(*exec.ExitError); ok {
		if status, ok := err.Sys().(syscall.WaitStatus); ok {
			result.ExitCode = status.ExitStatus()
		}
	}
	return result
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
