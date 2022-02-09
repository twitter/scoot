package os

import (
	"fmt"

	"os/exec"
	"strings"

	scootexecer "github.com/twitter/scoot/runner/execer"
)

// Used for mocking memCap monitoring
type ProcessWatcher interface {
	GetAndSetProcs() error
	MemUsage(int) (scootexecer.Memory, error)
}

type proc struct {
	pid  int
	pgid int
	ppid int
	rss  int
}

type procWatcher struct {
	allProcesses    map[int]proc
	processGroups   map[int][]proc
	parentProcesses map[int][]proc
}

func NewProcWatcher() *procWatcher {
	return &procWatcher{}
}

// Get a full list of processes running, including their pid, pgid, ppid, and memory usage, and set procWatcher's fields
func (opw *procWatcher) GetAndSetProcs() error {
	cmd := "ps -e -o pid= -o pgid= -o ppid= -o rss= | tr '\n' ';' | sed 's,;$,,'"
	psList := exec.Command("bash", "-c", cmd)
	b, err := psList.Output()
	if err != nil {
		return err
	}
	procs := strings.Split(string(b), ";")
	ap, pg, pp, err := parseProcs(procs)
	if err != nil {
		return err
	}
	opw.allProcesses = ap
	opw.processGroups = pg
	opw.parentProcesses = pp
	return nil
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
	relatedProcesses := []proc{}
	relatedProcessesMap := make(map[int]proc)
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

// Format processes into pgid and ppid groups for summation of memory usage
func parseProcs(procs []string) (allProcesses map[int]proc, processGroups map[int][]proc,
	parentProcesses map[int][]proc, err error) {
	allProcesses = make(map[int]proc)
	processGroups = make(map[int][]proc)
	parentProcesses = make(map[int][]proc)
	for idx := 0; idx < len(procs); idx++ {
		var p proc
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
