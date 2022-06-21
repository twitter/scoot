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

	"github.com/twitter/scoot/common/stats"
	scootexecer "github.com/twitter/scoot/runner/execer"

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
	getMemUtilization func() (int64, error)
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
	if getMemUtilization != nil {
		oe.getMemUtilization = getMemUtilization
	}
	return oe
}

// Start a command, monitor its memory, and return an &process wrapper for it
func (e *execer) Exec(command scootexecer.Command) (scootexecer.Process, error) {
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

	proc := &process{cmd: cmd, wg: &wg, ats: AbortTimeoutSec, LogTags: command.LogTags}
	if e.memCap > 0 {
		go e.monitorMem(proc, command.MemCh)
	}

	return proc, nil
}

// Periodically check to make sure memory constraints are respected,
// and clean up after ourselves when the process has completed
func (e *execer) monitorMem(p *process, memCh chan scootexecer.ProcessStatus) {
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
	memTicker := time.NewTicker(500 * time.Millisecond)
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
			if _, err := e.pw.GetProcs(); err != nil {
				log.Error(err)
			}
			var mem scootexecer.Memory
			// if not nil, use the provided function to get memory utilization,
			// otherwise get the memory usage of the current process and its subprocesses
			if e.getMemUtilization != nil {
				var memory int64
				memory, err = e.getMemUtilization()
				mem = scootexecer.Memory(memory)
			} else {
				mem, err = e.pw.MemUsage(pid)
			}
			if err != nil{
				log.Debugf("Error getting memory utilization: %s", err)
				e.stat.Gauge(stats.WorkerMemory).Update(-1)
				continue
			}
			e.stat.Gauge(stats.WorkerMemory).Update(int64(mem))
			// Abort process if calculated memory utilization is above memCap
			if mem >= e.memCap {
				msg := fmt.Sprintf("Cmd exceeded MemoryCap, aborting process %d: %d > %d (%v)", pid, mem, e.memCap, p.cmd.Args)
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
				p.result = &scootexecer.ProcessStatus{
					State:    scootexecer.COMPLETE,
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
					}).Infof("Memory utilization increased to %d, pid: %d", int(memUsagePct*100), pid)

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
						}).Tracef("ps after increased memory utilization for pid %d", pid)
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
