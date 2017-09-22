package runners

import (
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/runner/execer/execers"
	os_execer "github.com/twitter/scoot/runner/execer/os"
	"github.com/twitter/scoot/snapshot/snapshots"
)

func TestRun(t *testing.T) {
	defer teardown(t)
	r, _ := newRunner()
	assertRun(t, r, complete(0), "complete 0")
	assertRun(t, r, complete(1), "complete 1")
	if status, _, err := r.StatusAll(); len(status) != 1 {
		t.Fatalf("Expected history count of 1, got %d, err=%v", len(status), err)
	}
}

func TestOutput(t *testing.T) {
	defer teardown(t)
	r, _ := newRunner()
	stdoutExpected, stderrExpected := "hello world\n", "hello err\n"
	id := assertRun(t, r, complete(0),
		"stdout "+stdoutExpected, "stderr "+stderrExpected, "complete 0")
	st, _, err := r.Status(id)
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
	stdoutExpected, stderrExpected = "(?s).*SCOOT_CMD_LOG\nhello world\n$", "(?s).*SCOOT_CMD_LOG\nhello err\n$"
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := regexp.Match(stdoutExpected, stdoutActual); !ok {
		t.Fatalf("stdout was %q; expected %q", stdoutActual, stdoutExpected)
	}

	stderrFilename := strings.TrimPrefix(st.StderrRef, uriPrefix)
	stderrActual, err := ioutil.ReadFile(stderrFilename)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := regexp.Match(stderrExpected, stderrActual); !ok {
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
		t.Fatal("Expected: no resources available err.")
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
	st, _, err := r.Status(runID)
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
	str := `import time; exec("x=[]\nfor i in range(5):\n x.append(' ' * 10*1024*1024)\n time.sleep(.1)")`
	cmd := &runner.Command{Argv: []string{"python", "-c", str}}
	tmp, _ := temp.TempDirDefault()
	e := os_execer.NewBoundedExecer(execer.Memory(25*1024*1024), stats.NilStatsReceiver())
	r := NewSingleRunner(e, snapshots.MakeNoopFiler(tmp.Dir), nil, NewNullOutputCreator(), tmp, nil)
	if _, err := r.Run(cmd); err != nil {
		t.Fatalf(err.Error())
	}

	query := runner.Query{
		AllRuns: true,
		States:  runner.MaskForState(runner.FAILED),
	}
	// Travis may be slow, wait a super long time? This may also be necessary due to slow debug output from os_execer? TBD.
	if runs, _, err := r.Query(query, runner.Wait{Timeout: 5 * time.Second}); err != nil {
		t.Fatalf(err.Error())
	} else if len(runs) != 1 || !strings.Contains(runs[0].Error, "MemoryCap") {
		status, _, err := r.StatusAll()
		t.Fatalf("Expected result with FAILURE and matching err string, got: %v -- status %v -- err %v", runs, status, err)
	}
}

func TestStats(t *testing.T) {
	stat, statsReg := setupTest()
	args := []string{"sleep 50"}
	cmd := &runner.Command{Argv: args, SnapshotID: "dummySnapshotId"}
	tmp, _ := temp.TempDirDefault()
	e := execers.NewSimExecer()
	r := NewSingleRunner(e, snapshots.MakeNoopFiler(tmp.Dir), nil, NewNullOutputCreator(), tmp, stat)
	if _, err := r.Run(cmd); err != nil {
		t.Fatalf(err.Error())
	}

	query := runner.Query{
		AllRuns: true,
		States:  runner.DONE_MASK,
	}
	// wait for the run to finish
	status, svcStatus, err := r.Query(query, runner.Wait{Timeout: 5 * time.Second})
	log.WithFields(
		log.Fields{
			"statusLen": len(status),
			"svcStatus": svcStatus,
			"err":       err,
		}).Info("Received status")

	if !stats.StatsOk("", statsReg, t,
		map[string]stats.Rule{
			stats.WorkerUploadLatency_ms + ".avg":   {Checker: stats.FloatGTTest, Value: 0.0},
			stats.WorkerDownloadLatency_ms + ".avg": {Checker: stats.FloatGTTest, Value: 0.0},
			stats.WorkerUploads:                     {Checker: stats.Int64EqTest, Value: 1},
			stats.WorkerDownloads:                   {Checker: stats.Int64EqTest, Value: 1},
			stats.WorkerTaskLatency_ms + ".avg":     {Checker: stats.FloatGTTest, Value: 50.0},
		}) {
		t.Fatal("stats check did not pass.")
	}
}

func TestTimeout(t *testing.T) {
	stat, statsReg := setupTest()
	args := []string{"pause"}
	cmd := &runner.Command{Argv: args, SnapshotID: "dummySnapshotId", Timeout: 50 * time.Millisecond}
	tmp, _ := temp.TempDirDefault()
	e := execers.NewSimExecer()
	r := NewSingleRunner(e, snapshots.MakeNoopFiler(tmp.Dir), nil, NewNullOutputCreator(), tmp, stat)
	if _, err := r.Run(cmd); err != nil {
		t.Fatalf(err.Error())
	}

	query := runner.Query{
		AllRuns: true,
		States:  runner.DONE_MASK,
	}
	// wait for the run to finish
	status, _, _ := r.Query(query, runner.Wait{Timeout: 20 * time.Second})
	if len(status) != 1 {
		t.Fatalf("expected 1 status entry, got %d", len(status))
	}
	if status[0].State != runner.TIMEDOUT {
		t.Fatalf("expected timedout state, got %s", status[0].State.String())
	}

	if !stats.StatsOk("", statsReg, t,
		map[string]stats.Rule{
			stats.WorkerTaskLatency_ms + ".avg": {Checker: stats.FloatGTTest, Value: (50.0)},
		}) {
		t.Fatal("stats check did not pass.")
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
	r := NewSingleRunner(sim, snapshots.MakeInvalidFiler(), nil, outputCreator, tmpDir, nil)
	return r, sim
}

func setupTest() (stats.StatsReceiver, stats.StatsRegistry) {
	log.AddHook(hooks.NewContextHook())
	logrusLevel, _ := log.ParseLevel("debug")
	log.SetLevel(logrusLevel)

	statsReg := stats.NewFinagleStatsRegistry()
	regFn := func() stats.StatsRegistry { return statsReg }
	stat, _ := stats.NewCustomStatsReceiver(regFn, 0)

	return stat, statsReg
}
