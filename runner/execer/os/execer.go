package os

import (
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/wisechengyi/scoot/common/errors"
	"github.com/wisechengyi/scoot/common/stats"
	scootexecer "github.com/wisechengyi/scoot/runner/execer"

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
type execer struct {
	// Best effort monitoring of command to kill it if resident memory usage exceeds this cap
	memCap            scootexecer.Memory
	getMemUtilization func(int) (scootexecer.Memory, error)
	stat              stats.StatsReceiver
	pw                ProcessWatcher
}

// NewBoundedExecer returns an execer with a ProcGetter and, if non-zero values are provided, a memCap, overriding memory utilization function, and a StatsReceiver
func NewBoundedExecer(memCap scootexecer.Memory, getMemUtilization func() (int64, error), stat stats.StatsReceiver) *execer {
	oe := &execer{pw: NewProcWatcher()}
	if memCap != 0 {
		oe.memCap = memCap
	}
	if stat != nil {
		oe.stat = stat
	}
	// if not nil, use the provided function to get memory utilization,
	// otherwise get the memory usage of the current process and its subprocesses
	if getMemUtilization != nil {
		oe.getMemUtilization = func(int) (scootexecer.Memory, error) {
			mem, err := getMemUtilization()
			if err != nil {
				return 0, err
			}
			return scootexecer.Memory(mem), err
		}
	} else {
		oe.getMemUtilization = oe.pw.MemUsage
	}
	return oe
}

// Start a command, monitor its memory, and return an &process wrapper for it
func (e *execer) Exec(command scootexecer.Command) (scootexecer.Process, error) {
	if len(command.Argv) == 0 {
		return nil, fmt.Errorf("No command specified.")
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

	proc := &process{cmd: cmd, wg: &wg, ats: AbortTimeoutSec, LogTags: command.LogTags}
	if e.memCap > 0 {
		go e.monitorMem(proc, command.MemCh, command.Stderr)
	}

	return proc, nil
}

// Periodically check to make sure memory constraints are respected,
// and clean up after ourselves when the process has completed
func (e *execer) monitorMem(p *process, memCh chan scootexecer.ProcessStatus, stderr io.Writer) {
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
	log.WithFields(
		log.Fields{
			"pid":    pid,
			"tag":    p.Tag,
			"jobID":  p.JobID,
			"taskID": p.TaskID,
		}).Info("Monitoring memory")
	if _, err := e.pw.GetProcs(); err != nil {
		log.Error(err)
	}

	// check whether memory consumption is above threshold immediately on process start
	// mostly indicates that memory utilization was already above cap when the process started
	mem, err := e.getMemUtilization(pid)
	if err != nil {
		log.Debugf("Error getting memory utilization: %s", err)
		e.stat.Gauge(stats.WorkerMemory).Update(-1)
	} else if mem >= e.memCap {
		msg := fmt.Sprintf("Critical error detected. Initial memory utilization of worker is higher than threshold, aborting process %d: %d > %d (%v)",
			pid, mem, e.memCap, p.cmd.Args)
		e.stat.Counter(stats.WorkerHighInitialMemoryUtilization).Inc(1)
		p.mutex.Lock()
		p.result = &scootexecer.ProcessStatus{
			State:    scootexecer.FAILED,
			Error:    msg,
			ExitCode: errors.HighInitialMemoryUtilizationExitCode,
		}
		// log the process snapshot in worker log, as well as task stderr log
		e.pw.LogProcs(p, log.ErrorLevel, stderr)
		p.mutex.Unlock()
		e.memCapKill(p, mem, memCh)
		return
	}

	thresholdsIdx := 0
	reportThresholds := []float64{0, .25, .5, .75, .85, .9, .93, .95, .96, .97, .98, .99, 1}
	memTicker := time.NewTicker(500 * time.Millisecond)
	defer memTicker.Stop()
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
			if _, err := e.pw.GetProcs(); err != nil {
				log.Error(err)
			}
			mem, err := e.getMemUtilization(pid)
			if err != nil {
				p.mutex.Unlock()
				log.Debugf("Error getting memory utilization: %s", err)
				e.stat.Gauge(stats.WorkerMemory).Update(-1)
				continue
			}
			e.stat.Gauge(stats.WorkerMemory).Update(int64(mem))
			// Abort process if calculated memory utilization is above memCap
			if mem >= e.memCap {
				msg := fmt.Sprintf("Cmd exceeded MemoryCap, aborting process %d: %d > %d (%v)", pid, mem, e.memCap, p.cmd.Args)
				p.result = &scootexecer.ProcessStatus{
					State:    scootexecer.COMPLETE,
					Error:    msg,
					ExitCode: 1,
				}
				e.pw.LogProcs(p, log.ErrorLevel, stderr)
				p.mutex.Unlock()
				e.memCapKill(p, mem, memCh)
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
					}).Infof("Memory utilization increased to %d%%, pid: %d", int(memUsagePct*100), pid)
				e.pw.LogProcs(p, log.DebugLevel, nil)
				for memUsagePct > reportThresholds[thresholdsIdx] {
					thresholdsIdx++
				}
			}
			p.mutex.Unlock()
		}
	}
}

// memCapKill kills the process, handles cleanup and returns the process status to the invoker
func (e *execer) memCapKill(p *process, mem scootexecer.Memory, memCh chan scootexecer.ProcessStatus) {
	log.WithFields(
		log.Fields{
			"mem":    mem,
			"memCap": e.memCap,
			"args":   p.cmd.Args,
			"pid":    p.cmd.Process.Pid,
			"tag":    p.Tag,
			"jobID":  p.JobID,
			"taskID": p.TaskID,
		}).Info(p.result.Error)
	if memCh != nil {
		memCh <- *p.result
	}
	p.MemCapKill()
	// record memory after killing process
	postKillMem, err := e.getMemUtilization(p.cmd.Process.Pid)
	if err != nil {
		log.Debugf("Error getting memory utilization after killing process: %s", err)
		e.stat.Gauge(stats.WorkerMemory).Update(-1)
	}
	e.stat.Gauge(stats.WorkerMemory).Update(int64(postKillMem))
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
