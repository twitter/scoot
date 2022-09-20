package os

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"os/exec"
	"strings"

	log "github.com/sirupsen/logrus"
	scootexecer "github.com/twitter/scoot/runner/execer"
)

// Used for mocking memCap monitoring
type ProcessWatcher interface {
	GetProcs() (map[int]ProcInfo, error)
	MemUsage(int) (scootexecer.Memory, error)
	LogProcs(*process, log.Level, io.Writer)
}

type ProcInfo struct {
	pid  int
	pgid int
	ppid int
	rss  int
}

func (p ProcInfo) Pid() int {
	return p.pid
}

type procWatcher struct {
	allProcesses    map[int]ProcInfo
	processGroups   map[int][]ProcInfo
	parentProcesses map[int][]ProcInfo
}

func NewProcWatcher() *procWatcher {
	return &procWatcher{}
}

// Get a full list of processes running, including their pid, pgid, ppid, and memory usage, and set procWatcher's fields
func (opw *procWatcher) GetProcs() (map[int]ProcInfo, error) {
	cmd := "ps -e -o pid= -o pgid= -o ppid= -o rss= | tr '\n' ';' | sed 's,;$,,'"
	psList := exec.Command("bash", "-c", cmd)
	b, err := psList.Output()
	if err != nil {
		return nil, err
	}
	procs := strings.Split(string(b), ";")
	ap, pg, pp, err := parseProcs(procs)
	if err != nil {
		return nil, err
	}
	opw.allProcesses = ap
	opw.processGroups = pg
	opw.parentProcesses = pp
	return opw.allProcesses, nil
}

// Sums memory usage for a given process, including usage by related processes
func (opw *procWatcher) MemUsage(pid int) (scootexecer.Memory, error) {
	if _, ok := opw.allProcesses[pid]; !ok {
		return 0, fmt.Errorf("%d was not present in list of all processes", pid)
	}
	procGroupID := opw.allProcesses[pid].pgid
	// We have relatedProcesses & relatedProcessesMap b/c iterating over the range of a map while modifying it in place
	// introduces non-deterministic flaky behavior wrt memUsage summation. We add related procs to the relatedProcesses
	// slice iff they aren't present in relatedProcessesMap
	relatedProcesses := []ProcInfo{}
	relatedProcessesMap := make(map[int]ProcInfo)
	total := 0
	// Seed relatedProcesses with all procs from pid's process group
	for idx := 0; idx < len(opw.processGroups[procGroupID]); idx++ {
		p := opw.processGroups[procGroupID][idx]
		relatedProcesses = append(relatedProcesses, opw.allProcesses[p.pid])
		relatedProcessesMap[p.pid] = p
	}

	// Add all child procs of processes in pid's process group (and their child procs as well)
	for i := 0; i < len(relatedProcesses); i++ {
		rp := relatedProcesses[i]
		procPid := rp.pid
		for j := 0; j < len(opw.parentProcesses[procPid]); j++ {
			p := opw.parentProcesses[procPid][j]
			// Make sure it isn't already present in map
			if _, ok := relatedProcessesMap[p.pid]; !ok {
				relatedProcesses = append(relatedProcesses, opw.allProcesses[p.pid])
				relatedProcessesMap[p.pid] = p
			}
		}
	}

	// Add total rss usage of all relatedProcesses
	for _, proc := range relatedProcessesMap {
		total += proc.rss
	}
	return scootexecer.Memory(total * bytesToKB), nil
}

// LogProcs logs the process snapshot of the current process along with other running processes for the user in the worker log,
// at the specified level. Also writes to the writer, if provided
func (opw *procWatcher) LogProcs(p *process, level log.Level, w io.Writer) {
	if !log.IsLevelEnabled(level) {
		return
	}

	// log output with timeout since it seems CombinedOutput() sometimes fails to return.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	ps, err := exec.CommandContext(ctx, "ps", "-u", os.Getenv("USER"), "-opid,sess,ppid,pgid,rss,args", "--sort=-rss").CombinedOutput()

	log.WithFields(
		log.Fields{
			"pid":    p.cmd.Process.Pid,
			"ps":     string(ps),
			"err":    err,
			"errCtx": ctx.Err(),
			"tag":    p.Tag,
			"jobID":  p.JobID,
			"taskID": p.TaskID,
		}).Log(level, fmt.Sprintf("ps after increased memory utilization for pid %d", p.cmd.Process.Pid))

	if w != nil {
		w.Write([]byte(fmt.Sprintf("\nps after increased memory utilization for pid %d:\n\n", p.cmd.Process.Pid)))
		w.Write(ps)
	}

	cancel()
}

// Format processes into pgid and ppid groups for summation of memory usage
func parseProcs(procs []string) (allProcesses map[int]ProcInfo, processGroups map[int][]ProcInfo,
	parentProcesses map[int][]ProcInfo, err error) {
	allProcesses = make(map[int]ProcInfo)
	processGroups = make(map[int][]ProcInfo)
	parentProcesses = make(map[int][]ProcInfo)
	for idx := 0; idx < len(procs); idx++ {
		var p ProcInfo
		n, err := fmt.Sscanf(procs[idx], "%d %d %d %d", &p.pid, &p.pgid, &p.ppid, &p.rss)
		if err != nil {
			return nil, nil, nil, err
		}
		if n != 4 {
			return nil, nil, nil, fmt.Errorf("Error parsing output, expected 4 assigments, but only received %d. %v", n, procs)
		}
		allProcesses[p.pid] = p
		processGroups[p.pgid] = append(processGroups[p.pgid], p)
		parentProcesses[p.ppid] = append(parentProcesses[p.ppid], p)
	}
	return allProcesses, processGroups, parentProcesses, nil
}
