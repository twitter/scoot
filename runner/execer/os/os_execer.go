package os

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner/execer"

	log "github.com/sirupsen/logrus"
)

const bytesToKB = 1024

func NewExecer() *osExecer {
	return &osExecer{pg: &osProcGetter{}}
}

// For now memory can be capped on a per-execer basis rather than a per-command basis.
// This is ok since we currently (Q1 2017) only support one run at a time in our codebase.
func NewBoundedExecer(memCap execer.Memory, stat stats.StatsReceiver) *osExecer {
	return &osExecer{memCap: memCap, stat: stat.Scope("osexecer"), pg: &osProcGetter{}}
}

type osExecer struct {
	// Best effort monitoring of command to kill it if resident memory usage exceeds this cap. Ignored if zero.
	memCap execer.Memory
	stat   stats.StatsReceiver
	pg     procGetter
}

type procGetter interface {
	getProcs() (map[int]proc, map[int]proc, map[int][]proc, map[int][]proc, error)
	parseProcs([]string) (map[int]proc, map[int]proc, map[int][]proc, map[int][]proc, error)
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

	proc := &osProcess{cmd: cmd, wg: &wg, LogTags: command.LogTags}
	if e.memCap > 0 {
		go e.monitorMem(proc, command.MemCh)
	}
	return proc, nil
}

type osProcess struct {
	cmd    *exec.Cmd
	wg     *sync.WaitGroup
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
					State:    execer.COMPLETE,
					Error:    msg,
					ExitCode: 1,
				}
				if memCh != nil {
					memCh <- *p.result
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

				// Debug output with timeout since it seems CombinedOutput() sometimes fails to return.
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
					}).Debugf("ps after increasing mem_cap utilization for pid %d", pid)
				cancel()

				for memUsagePct > reportThresholds[thresholdsIdx] {
					thresholdsIdx++
				}
			}
			p.mutex.Unlock()
		}
	}
}

type proc struct {
	pid  int
	pgid int
	ppid int
	rss  int
}

type osProcGetter struct{}

// Query for all sets of (pid, pgid, ppid, rss). Given a pid, find all processes with pid as its pgid or ppid.
// Given this list of pids, find all processes with a pgid or ppid in that set, and modify the set in place.
// From there, sum the memory of all processes in aforementioned set.
func (e *osExecer) memUsage(pid int) (execer.Memory, error) {
	allProcesses, relatedProcesses, processGroups, parentProcesses, err := e.pg.getProcs()
	if err != nil {
		return 0, nil
	}
	if _, ok := allProcesses[pid]; !ok {
		return 0, fmt.Errorf("%d was not present in list of all processes", pid)
	}
	procGroupID := allProcesses[pid].pgid
	total := 0
	for idx := 0; idx < len(processGroups[procGroupID]); idx += 1 {
		p := processGroups[procGroupID][idx]
		relatedProcesses[p.pid] = allProcesses[p.pid]
	}
	for procPid, _ := range relatedProcesses {
		for idx := 0; idx < len(parentProcesses[procPid]); idx += 1 {
			p := parentProcesses[pid][idx]
			relatedProcesses[parentProcesses[procPid][idx].pid] = allProcesses[p.pid]
		}
	}
	for _, proc := range relatedProcesses {
		total += proc.rss
	}
	return execer.Memory(total * bytesToKB), nil
}

func (pg *osProcGetter) getProcs() (
	allProcesses map[int]proc, relatedProcesses map[int]proc, processGroups map[int][]proc,
	parentProcesses map[int][]proc, err error) {
	cmd := "ps -e -o pid= -o pgid= -o ppid= -o rss= | tr '\n' ';' | sed 's,;$,,'"
	psList := exec.Command("bash", "-c", cmd)
	b, err := psList.Output()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	procs := strings.Split(string(b), ";")
	return pg.parseProcs(procs)
}

func (pg *osProcGetter) parseProcs(procs []string) (allProcesses map[int]proc, relatedProcesses map[int]proc, processGroups map[int][]proc,
	parentProcesses map[int][]proc, err error) {
	allProcesses = make(map[int]proc)
	relatedProcesses = make(map[int]proc)
	processGroups = make(map[int][]proc)
	parentProcesses = make(map[int][]proc)
	for idx := 0; idx < len(procs); idx += 1 {
		var p proc
		n, err := fmt.Sscanf(procs[idx], "%d %d %d %d", &p.pid, &p.pgid, &p.ppid, &p.rss)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if n != 4 {
			return nil, nil, nil, nil, fmt.Errorf("Error parsing output, expected 4 assigments, but only received %d. %v", n, procs)
		}
		allProcesses[p.pid] = p
		processGroups[p.pgid] = append(processGroups[p.pgid], p)
		parentProcesses[p.ppid] = append(parentProcesses[p.ppid], p)
	}
	return allProcesses, relatedProcesses, processGroups, parentProcesses, nil
}

/*
Wait for the process to finish.

If the command finishes without error return the status COMPLETE and exit Code 0.

If the command fails, and we can get the exit code from the command, return COMPLETE with the failing exit code.

if the command fails and we cannot get the exit code from the command, return FAILED and the error
that prevented getting the exit code.
*/
func (p *osProcess) Wait() (result execer.ProcessStatus) {
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

	// Debug output with timeout since it seems CombinedOutput() sometimes fails to return.
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
		}).Debugf("Current ps for pid %d", pid)
	cancel()

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
