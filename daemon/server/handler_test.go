// +build daemon_test

// daemon_test is disabled in travis-ci

package server

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	execer "github.com/twitter/scoot/runner/execer/os"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/snapshot/snapshots"
)

// Run through an example daemon use case.
// Constructs snapshots for scripts & resources, runs those scripts, and validates new snapshot contents.
func TestDaemonExample(t *testing.T) {
	// Initialize types required to construct a daemon server handler.
	filerTmp, _ := temp.NewTempDir(os.TempDir(), "TestDaemonExample_filer")
	localTmp, _ := temp.NewTempDir(os.TempDir(), "TestDaemonExample_localpath")
	out, _ := runners.NewHttpOutputCreator(localTmp, "")
	filer := snapshots.MakeTempFiler(filerTmp)
	ex := execer.NewExecer()

	run := runners.NewSingleRunner(ex, filer, nil, out, filerTmp, nil)
	handler := NewHandler(run, filer, 50*time.Millisecond)

	// Populate the paths we want to ingest.
	okScript := filepath.Join(localTmp.Dir, "ok.sh")
	failScript := filepath.Join(localTmp.Dir, "fail.sh")
	resource := filepath.Join(localTmp.Dir, "resource.txt")
	ioutil.WriteFile(okScript, []byte("#!/bin/sh\nls resource.txt"), os.ModePerm)
	ioutil.WriteFile(failScript, []byte("#!/bin/sh\nls resource.txt"), os.ModePerm)
	ioutil.WriteFile(resource, []byte("content"), os.ModePerm)

	// Ingest scripts into their own snapshots. The 'fail' snapshot will be missing resource.txt.
	var err error
	var okId, failId string
	okId, err = handler.CreateSnapshot(filepath.Join(localTmp.Dir, "*"))
	if err != nil {
		t.Fatal("failure creating 'ok' snapshot.", err)
	}
	failId, err = handler.CreateSnapshot(failScript)
	if err != nil {
		t.Fatal("failure creating 'fail' snapshot.", err)
	}

	// Run scripts serially in their respective snapshots. Block until each run finishes.
	var okStatus, failStatus runner.RunStatus
	okStatus, err = handler.Run(&runner.Command{Argv: []string{"./ok.sh"}, Timeout: 500 * time.Millisecond, SnapshotID: okId})
	if err != nil {
		t.Fatal("failure running 'ok' snapshot.", err)
	}
	okStatuses := handler.Poll([]runner.RunID{okStatus.RunID}, 500*time.Millisecond, false)
	if len(okStatuses) != 1 {
		t.Fatal("failure polling 'ok' run.", len(okStatuses))
	}

	//...
	failStatus, err = handler.Run(&runner.Command{Argv: []string{"./fail.sh"}, Timeout: 500 * time.Millisecond, SnapshotID: failId})
	if err != nil {
		t.Fatal("failure running 'fail' snapshot.", err)
	}
	failStatuses := handler.Poll([]runner.RunID{failStatus.RunID}, 500*time.Millisecond, false)
	if len(failStatuses) != 1 {
		t.Fatal("failure polling 'fail' run.", len(failStatuses))
	}

	// Make sure 'ok' and 'fail' returned the correct exit code.
	if okStatuses[0].ExitCode != 0 {
		t.Fatal("failure checking exit code of 'ok' run.", okStatuses[0].ExitCode)
	}
	if failStatuses[0].ExitCode == 0 {
		t.Fatal("failure checking exit code of 'fail' run.", failStatuses[0].ExitCode)
	}

	// Checkout result snapshots for both runs.
	okDir := filepath.Join(localTmp.Dir, "okco")
	failDir := filepath.Join(localTmp.Dir, "failco")
	err = handler.CheckoutSnapshot(okStatuses[0].SnapshotID, okDir)
	if err != nil {
		t.Fatal("failure checking out 'ok' result snapshot.", err)
	}
	err = handler.CheckoutSnapshot(failStatuses[0].SnapshotID, failDir)
	if err != nil {
		t.Fatal("failure checking out 'fail' result snapshot.", err)
	}

	// Check that 'ok' and 'fail' populated only STDOUT or STDERR respectively.
	assertFileContains(filepath.Join(okDir, "STDOUT"), "resource.txt\n", "ok", t)
	assertFileContains(filepath.Join(okDir, "STDERR"), "", "ok", t)
	assertFileContains(filepath.Join(failDir, "STDOUT"), "", "fail", t)
	assertFileContains(filepath.Join(failDir, "STDERR"), "No such file or directory\n", "fail", t)

	//TODO: test OutputPlan.
}

func assertFileContains(path, contents, msg string, t *testing.T) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(msg + ", readfile: \"" + err.Error() + "\" -- " + path)
	}
	if match, _ := regexp.Match(contents, b); !match {
		t.Fatal(msg + ", contents: \"" + string(b) + "\" -- " + path)
	}
}
