package os

import (
	"fmt"
	"testing"

	"github.com/twitter/scoot/runner/execer"
	// log "github.com/sirupsen/logrus"
)

func NewBoundedTestExecer(memCap execer.Memory, pg procGetter) *osExecer {
	return &osExecer{memCap: memCap, pg: pg}
}

// Tests that single process memory usage is counted
func TestOsExecerMemUsage(t *testing.T) {
	limit := 10
	pg := &testProcGetter{procs: []string{fmt.Sprintf("1 1 1 %d", limit)}}
	e := NewBoundedTestExecer(execer.Memory(limit*bytesToKB), pg)
	mem, err := e.memUsage(1)
	if mem != execer.Memory(limit*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that memory of processes spawned by a process in original process's process group are counted
func TestParentProcGroup(t *testing.T) {
	limit := 10
	pg := &testProcGetter{procs: []string{fmt.Sprintf("1 1 1 %d", limit), fmt.Sprintf("2 1 1 %d", limit), fmt.Sprintf("3 2 2 %d", limit)}}
	e := NewBoundedTestExecer(execer.Memory(limit*bytesToKB), pg)
	mem, err := e.memUsage(1)
	if mem != execer.Memory(limit*3*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that memory of processes within process group are counted
func TestProcGroup(t *testing.T) {
	limit := 10
	pg := &testProcGetter{procs: []string{fmt.Sprintf("1 1 1 %d", limit), fmt.Sprintf("2 1 1 %d", limit), fmt.Sprintf("3 1 2 %d", limit)}}
	e := NewBoundedTestExecer(execer.Memory(limit*bytesToKB), pg)
	mem, err := e.memUsage(1)
	if mem != execer.Memory(limit*3*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that memory of unrelated processes are not counted
func TestUnrelatedProcs(t *testing.T) {
	limit := 10
	pg := &testProcGetter{procs: []string{
		fmt.Sprintf("1 1 1 %d", limit), fmt.Sprintf("2 1 1 %d", limit), fmt.Sprintf("3 1 2 %d", limit), fmt.Sprintf("100 100 100 100")}}
	e := NewBoundedTestExecer(execer.Memory(limit*bytesToKB), pg)
	mem, err := e.memUsage(1)
	if mem != execer.Memory(limit*3*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that processes related through original process's pgid and their children are counted
func TestParentProcGroupAndChildren(t *testing.T) {
	limit := 10
	pg := &testProcGetter{procs: []string{
		fmt.Sprintf("0 0 0 %d", limit), fmt.Sprintf("1 0 1 %d", limit),
		fmt.Sprintf("2 1 1 %d", limit), fmt.Sprintf("3 1 2 %d", limit),
		fmt.Sprintf("100 0 0 %d", limit), fmt.Sprintf("101 100 100 %d", limit),
		fmt.Sprintf("1000 1000 1000 %d", limit)}}
	e := NewBoundedTestExecer(execer.Memory(limit*bytesToKB), pg)
	mem, err := e.memUsage(1)
	if mem != execer.Memory(limit*6*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

type testProcGetter struct {
	procs []string // pid, pgid, ppid, rss format
	osProcGetter
}

func (pg *testProcGetter) getProcs() (
	allProcesses map[int]proc, relatedProcesses map[int]proc, processGroups map[int][]proc,
	parentProcesses map[int][]proc, err error) {
	// log.Info("k")
	return pg.parseProcs(pg.procs)
}
