package os

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wisechengyi/scoot/common/log/hooks"
	scootexecer "github.com/wisechengyi/scoot/runner/execer"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.AddHook(hooks.NewContextHook())
	logrusLevel, _ := log.ParseLevel("debug")
	log.SetLevel(logrusLevel)
}

func NewBoundedTestExecer(memCap scootexecer.Memory, pw ProcessWatcher) *execer {
	return &execer{memCap: memCap, pw: pw}
}

// Tests that single process memory usage is counted
func TestExecerMemUsage(t *testing.T) {
	rss := 10
	pw := &testProcWatcher{procs: []string{fmt.Sprintf("1 1 1 %d", rss)}}
	_, err := pw.GetProcs()
	if err != nil {
		t.Fatal(err)
	}
	mem, err := pw.MemUsage(1)
	if err != nil {
		t.Fatal(err)
	}
	if mem != scootexecer.Memory(rss*bytesToKB) {
		t.Fatalf("Unexpected rss value: %v != %v", rss*bytesToKB, mem)
	}
}

// Tests that memory of processes spawned by a process in original process's process group are counted
func TestParentProcGroup(t *testing.T) {
	rss := 10
	pw := &testProcWatcher{procs: []string{fmt.Sprintf("1 1 1 %d", rss), fmt.Sprintf("2 1 1 %d", rss), fmt.Sprintf("3 2 2 %d", rss)}}
	_, err := pw.GetProcs()
	if err != nil {
		t.Fatal(err)
	}
	mem, err := pw.MemUsage(1)
	if mem != scootexecer.Memory(rss*3*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that memory of processes within process group are counted
func TestProcGroup(t *testing.T) {
	rss := 10
	pw := &testProcWatcher{procs: []string{fmt.Sprintf("1 1 1 %d", rss), fmt.Sprintf("2 1 1 %d", rss), fmt.Sprintf("3 1 2 %d", rss)}}
	_, err := pw.GetProcs()
	if err != nil {
		t.Fatal(err)
	}
	mem, err := pw.MemUsage(1)
	if mem != scootexecer.Memory(rss*3*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that memory of unrelated processes are not counted
func TestUnrelatedProcs(t *testing.T) {
	rss := 10
	pw := &testProcWatcher{procs: []string{
		fmt.Sprintf("1 1 1 %d", rss), fmt.Sprintf("2 1 1 %d", rss), fmt.Sprintf("3 1 2 %d", rss), fmt.Sprintf("100 100 100 100")}}
	_, err := pw.GetProcs()
	if err != nil {
		t.Fatal(err)
	}
	mem, err := pw.MemUsage(1)
	if mem != scootexecer.Memory(rss*3*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem", err, mem)
	}
}

// Tests that processes related through original process's pwid and their children are counted
func TestParentProcGroupAndChildren(t *testing.T) {
	rss := 10
	pw := &testProcWatcher{procs: []string{
		fmt.Sprintf("0  0      0  %d", rss), fmt.Sprintf("1   0 1 %d", rss),
		fmt.Sprintf("2 1       1 %d", rss), fmt.Sprintf("3  2    1      %d", rss),
		fmt.Sprintf("4  3   3 %d", rss), fmt.Sprintf("5  2   3 %d", rss),
		fmt.Sprintf("6  5   5 %d", rss), fmt.Sprintf("100    0   0  %d ", rss),
		fmt.Sprintf("   101   100  100  %d", rss), fmt.Sprintf("  1000   1000      1001 %d   ", rss)}}
	_, err := pw.GetProcs()
	if err != nil {
		t.Fatal(err)
	}
	mem, err := pw.MemUsage(1)
	if mem != scootexecer.Memory(rss*9*bytesToKB) || err != nil {
		t.Fatalf("%v: %v mem\nallProcesses:\n\t%v\nprocessGroups:\n\t%v\nparentProcesses:\n\t%v", err, mem, pw.allProcesses, pw.processGroups, pw.parentProcesses)
	}
}

func TestAbortSigterm(t *testing.T) {
	e := NewBoundedExecer(0, nil, nil)
	cmd := scootexecer.Command{
		Argv: []string{"sleep", "1000"},
	}

	// test without Wait(). In this case result.Error should get set to
	proc, err := e.Exec(cmd)
	if err != nil {
		t.Fatal(err)
	}
	proc.(*process).ats = 1

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
	proc.(*process).ats = 1

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
	e := NewBoundedTestExecer(0, &procWatcher{})
	var stdout, stderr bytes.Buffer
	cmd := scootexecer.Command{
		Argv:   []string{"sh", "./trap_script.sh"},
		Stderr: &stderr,
		Stdout: &stdout,
	}

	proc, err := e.Exec(cmd)
	if err != nil {
		t.Fatal(err)
	}
	pid := proc.(*process).cmd.Process.Pid
	proc.(*process).ats = 1

	time.Sleep(500 * time.Millisecond)
	_, err = e.pw.GetProcs()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := e.pw.MemUsage(pid)
	if err != nil {
		t.Fatal(err)
	}
	if usage == 0 {
		t.Fatalf("Expected usage to be >0 for process %d", pid)
	}

	proc.Abort()
	_, err = e.pw.GetProcs()
	if err != nil {
		t.Fatal(err)
	}
	usage, err = e.pw.MemUsage(pid)
	if usage != 0 {
		t.Fatalf("Expected memUsage to be 0 after Abort & Kill, was %d", usage)
	}

	time.Sleep(100 * time.Millisecond)
	_, err = e.pw.GetProcs()
	if err != nil {
		t.Fatal(err)
	}
	usage, err = e.pw.MemUsage(pid)
	if err == nil {
		t.Fatalf("Expected %d to not exist as a process anymore.", pid)
	}
	if usage != 0 {
		t.Fatalf("Expected memUsage to be 0 after Abort & Kill, was %d", usage)
	}
}

type testProcWatcher struct {
	procs []string // pid, pwid, ppid, rss format
	procWatcher
}

func (pw *testProcWatcher) GetProcs() (map[int]ProcInfo, error) {
	ap, pws, pps, err := parseProcs(pw.procs)
	pw.allProcesses = ap
	pw.processGroups = pws
	pw.parentProcesses = pps
	return pw.allProcesses, err
}
