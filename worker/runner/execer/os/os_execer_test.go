package os

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/worker/runner/execer"

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

func TestAbortSigterm(t *testing.T) {
	e := NewExecer()
	cmd := execer.Command{
		Argv: []string{"sleep", "1000"},
	}

	// test without Wait(). In this case result.Error should get set to
	proc, err := e.Exec(cmd)
	if err != nil {
		t.Fatal(err)
	}
	proc.(*osProcess).ats = 1

	res := proc.Abort()
	// error string could be implementation dependent
	if !strings.Contains(res.Error, "SIGTERM") {
		t.Fatalf("Expected error set with SIGTERM message, got: %s", res.Error)
	}

	// Abort and Wait can collide in the real world.
	// However, in here, it's a data race.
	// Uncomment and `go test` without the -race flag to verify this behavior.
	/*proc, err = e.Exec(cmd)
	if err != nil {
		t.Fatal(err)
	}
	proc.(*osProcess).ats = 1

	go func() {
		proc.Wait()
	}()
	time.Sleep(100 * time.Millisecond)
	res = proc.Abort()
	if !strings.Contains(res.Error, "SIGTERM") {
		t.Fatalf("Expected error set with SIGTERM message, got: %s", res.Error)
	}*/
}

func TestAbortCatch(t *testing.T) {
	e := NewBoundedTestExecer(0, &osProcGetter{})
	var stdout, stderr bytes.Buffer
	cmd := execer.Command{
		Argv:   []string{"sh", "./trap_script.sh"},
		Stderr: &stderr,
		Stdout: &stdout,
	}

	proc, err := e.Exec(cmd)
	if err != nil {
		t.Fatal(err)
	}
	pid := proc.(*osProcess).cmd.Process.Pid
	proc.(*osProcess).ats = 1

	time.Sleep(500 * time.Millisecond)
	usage, err := e.memUsage(pid)
	if err != nil {
		t.Fatal(err)
	}
	if usage == 0 {
		t.Fatalf("Expected usage to be >0 for process %d", pid)
	}

	proc.Abort()
	usage, err = e.memUsage(pid)
	if usage != 0 {
		t.Fatalf("Expected memUsage to be 0 after Abort & Kill, was %d", usage)
	}

	time.Sleep(100 * time.Millisecond)
	usage, err = e.memUsage(pid)
	if err == nil {
		t.Fatalf("Expected %d to not exist as a process anymore.", pid)
	}
	if usage != 0 {
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
