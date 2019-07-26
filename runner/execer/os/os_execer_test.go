package os

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/runner/execer"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.AddHook(hooks.NewContextHook())
	logrusLevel, _ := log.ParseLevel("debug")
	log.SetLevel(logrusLevel)
}

func NewBoundedTestExecer(memCap execer.Memory, pg procGetter) *osExecer {
	return &osExecer{memCap: memCap, pg: pg}
}

// Tests that single process memory usage is counted
func TestOsExecerMemUsage(t *testing.T) {
	rss := 10
	pg := &testProcGetter{procs: []string{fmt.Sprintf("1 1 1 %d", rss)}}
	e := NewBoundedTestExecer(execer.Memory(0), pg)
	mem, err := e.memUsage(1)
	if mem != execer.Memory(rss*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that memory of processes spawned by a process in original process's process group are counted
func TestParentProcGroup(t *testing.T) {
	rss := 10
	pg := &testProcGetter{procs: []string{fmt.Sprintf("1 1 1 %d", rss), fmt.Sprintf("2 1 1 %d", rss), fmt.Sprintf("3 2 2 %d", rss)}}
	e := NewBoundedTestExecer(execer.Memory(0), pg)
	mem, err := e.memUsage(1)
	if mem != execer.Memory(rss*3*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that memory of processes within process group are counted
func TestProcGroup(t *testing.T) {
	rss := 10
	pg := &testProcGetter{procs: []string{fmt.Sprintf("1 1 1 %d", rss), fmt.Sprintf("2 1 1 %d", rss), fmt.Sprintf("3 1 2 %d", rss)}}
	e := NewBoundedTestExecer(execer.Memory(0), pg)
	mem, err := e.memUsage(1)
	if mem != execer.Memory(rss*3*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that memory of unrelated processes are not counted
func TestUnrelatedProcs(t *testing.T) {
	rss := 10
	pg := &testProcGetter{procs: []string{
		fmt.Sprintf("1 1 1 %d", rss), fmt.Sprintf("2 1 1 %d", rss), fmt.Sprintf("3 1 2 %d", rss), fmt.Sprintf("100 100 100 100")}}
	e := NewBoundedTestExecer(execer.Memory(0), pg)
	mem, err := e.memUsage(1)
	if mem != execer.Memory(rss*3*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that processes related through original process's pgid and their children are counted
func TestParentProcGroupAndChildren(t *testing.T) {
	rss := 10
	pg := &testProcGetter{procs: []string{
		fmt.Sprintf("0  0      0  %d", rss), fmt.Sprintf("1   0 1 %d", rss),
		fmt.Sprintf("2 1       1 %d", rss), fmt.Sprintf("3  2    1      %d", rss),
		fmt.Sprintf("4  3   3 %d", rss), fmt.Sprintf("5  2   3 %d", rss),
		fmt.Sprintf("6  5   5 %d", rss), fmt.Sprintf("100    0   0  %d ", rss),
		fmt.Sprintf("   101   100  100  %d", rss), fmt.Sprintf("  1000   1000      1001 %d   ", rss)}}
	e := NewBoundedTestExecer(execer.Memory(0), pg)
	mem, err := e.memUsage(1)
	if mem != execer.Memory(rss*9*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem\nallProcesses:\n\t%v\nprocessGroups:\n\t%v\nparentProcesses:\n\t%v", err, mem, pg.allProcesses, pg.processGroups, pg.parentProcesses)
	}
}

func TestAbortCatch(t *testing.T) {
	e := NewBoundedTestExecer(0, &testProcGetter{})
	var stdout, stderr bytes.Buffer
	cmd := execer.Command{
		Argv:   []string{"bash", "-c", "trap ':' SIGTERM && while :; do sleep 1; done"},
		Stderr: &stderr,
		Stdout: &stdout,
	}
	proc, err := e.Exec(cmd)
	if err != nil {
		t.Fatal(err)
	}
	proc.Abort()
	usage, err := e.memUsage(proc.(*osProcess).cmd.Process.Pid)
	if usage != 0 && err == nil {
		t.Fatalf("Expected memUsage to be 0 after Abort & Kill, was %d", usage)
	}
}

type testProcGetter struct {
	procs []string // pid, pgid, ppid, rss format
	osProcGetter
	allProcesses    map[int]proc
	processGroups   map[int][]proc
	parentProcesses map[int][]proc
}

func (pg *testProcGetter) getProcs() (
	allProcesses map[int]proc, processGroups map[int][]proc,
	parentProcesses map[int][]proc, err error) {
	ap, pgs, pps, err := pg.parseProcs(pg.procs)
	pg.allProcesses = ap
	pg.processGroups = pgs
	pg.parentProcesses = pps
	return ap, pgs, pps, err
}
