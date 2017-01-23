package runners

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/execers"
	os_execer "github.com/scootdev/scoot/runner/execer/os"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

func TestRun(t *testing.T) {
	defer teardown(t)
	r, _ := newRunner()
	firstID := assertRun(t, r, complete(0), "complete 0")
	assertRun(t, r, complete(1), "complete 1")
	// Now make sure that the first results are still available
	assertWait(t, r, firstID, complete(0), "complete 0")
}

func TestOutput(t *testing.T) {
	defer teardown(t)
	r, _ := newRunner()
	stdoutExpected, stderrExpected := "hello world\n", "hello err\n"
	id := assertRun(t, r, complete(0),
		"stdout "+stdoutExpected, "stderr "+stderrExpected, "complete 0")
	st, err := r.Status(id)
	if err != nil {
		t.Fatal(err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	uriPrefix := "file://" + hostname
	stdoutFilename := strings.TrimPrefix(st.StdoutRef, uriPrefix)
	stdoutActual, err := ioutil.ReadFile(stdoutFilename)
	if err != nil {
		t.Fatal(err)
	}
	if stdoutExpected != string(stdoutActual) {
		t.Fatalf("stdout was %q; expected %q", stdoutActual, stdoutExpected)
	}

	stderrFilename := strings.TrimPrefix(st.StderrRef, uriPrefix)
	stderrActual, err := ioutil.ReadFile(stderrFilename)
	if err != nil {
		t.Fatal(err)
	}
	if stderrExpected != string(stderrActual) {
		t.Fatalf("stderr was %q; expected %q", stderrActual, stderrExpected)
	}
}

func TestSimul(t *testing.T) {
	defer teardown(t)
	r, sim := newRunner()
	firstArgs := []string{"pause", "complete 0"}
	firstRun := run(t, r, firstArgs)
	assertWait(t, r, firstRun, running(), firstArgs...)

	// Now that one is running, try running a second
	secondArgs := []string{"complete 3"}
	cmd := &runner.Command{}
	cmd.Argv = secondArgs
	_, err := r.Run(cmd)
	if err == nil {
		t.Fatal(err)
	}

	sim.Resume()
	assertWait(t, r, firstRun, complete(0), firstArgs...)
}

func TestAbort(t *testing.T) {
	defer teardown(t)
	r, _ := newRunner()
	args := []string{"pause", "complete 0"}
	runID := run(t, r, args)
	assertWait(t, r, runID, running(), args...)
	r.Abort(runID)
	// use r.Status instead of assertWait so that we make sure it's aborted immediately, not eventually
	st, err := r.Status(runID)
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, st, aborted(), args...)

	st, err = r.Abort(runner.RunID("not-a-run-id"))
	if err == nil {
		t.Fatal(err)
	}
}

func TestMemCap(t *testing.T) {
	// Command to increase memory by 10MB every .1s until we hit 50MB after .5s.
	// Test that limiting the memory to 25MB causes the command to abort.
	str := "python -c \"import time\nx=[]\nfor i in range(5):\n x.append(' ' * 10*1024*1024)\n time.sleep(.1)\" &"
	cmd := &runner.Command{Argv: []string{"bash", "-c", str}}
	tmp, _ := temp.TempDirDefault()
	e := os_execer.NewBoundedExecer(execer.Memory(25*1024*1024), stats.NilStatsReceiver())
	r := NewSingleRunner(e, snapshots.MakeNoopFiler(tmp.Dir), NewNullOutputCreator(), tmp)
	if _, err := r.Run(cmd); err != nil {
		t.Fatalf(err.Error())
	}

	query := runner.Query{
		AllRuns: true,
		States:  runner.MaskForState(runner.FAILED),
	}
	if runs, err := r.Query(query, runner.Wait{Timeout: 2 * time.Second}); err != nil { //travis may be slow, wait a super long time?
		t.Fatalf(err.Error())
	} else if len(runs) != 1 || !strings.Contains(runs[0].Error, "MemoryCap") {
		t.Fatalf("Expected result with FAILURE and matching err string, got: %v", runs)
	}
}

func newRunner() (runner.Service, *execers.SimExecer) {
	sim := execers.NewSimExecer()
	tmpDir, err := temp.TempDirDefault()
	if err != nil {
		panic(err)
	}

	outputCreator, err := NewHttpOutputCreator(tmpDir, "")
	if err != nil {
		panic(err)
	}
	r := NewSingleRunner(sim, snapshots.MakeInvalidFiler(), outputCreator, tmpDir)
	return r, sim
}
