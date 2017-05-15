package server

import (
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/os"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/snapshots"

	sysOS "os"
	"strings"
	"testing"
	"time"
)

type testEnv struct {
	execer            execer.Execer
	tmpDirForCheckout string
	checkouter        snapshot.Checkouter
	outputCreator     runner.OutputCreator
	localScoot        *LocalScoot
}

// validate that we get the correct error message when no runners have been created
func TestNoRunners(t *testing.T) {

	// setup the test environment with no runners
	timeout := 2 * time.Second
	testEnv := setup(t, 0, timeout)
	defer teardown(t, testEnv)
	localScoot := testEnv.localScoot

	// run the test scenario
	cmd := []string{"echo", "hello world"}
	var emptyOutputStrategy OutputStrategy = ""
	_, err := localScoot.Run("1", cmd, emptyOutputStrategy)
	expectedMsg := "No runners available"
	if strings.Compare(err.Error(), expectedMsg) != 0 {
		t.Fatalf("test failed when no runners where created expected %s, got %s\n", expectedMsg, err.Error())
	}
}

// Start a runner whose processing will not be finished by the
// time a second request comes in.
// Validate that the second request returns a message indicating that a runner could not be found
func TestAllRunnersBusyProcessing(t *testing.T) {

	// setup the test environment with 1 runner
	timeout := 2 * time.Second
	testEnv := setup(t, 1, timeout)
	defer teardown(t, testEnv)
	localScoot := testEnv.localScoot

	// run the test scenario
	// start a runner - it should start successfully
	cmd := []string{"sleep", "1"}
	var emptyOutputStrategy OutputStrategy = ""
	runId, err := localScoot.Run("", cmd, emptyOutputStrategy)
	// validate successful start
	if runId == "" {
		t.Fatal("failed running the first sleep: no runId returned")
	}
	if err != nil {
		t.Fatalf("error returned on the first sleep: %s\n", err.Error())
	}

	// start the second runner - it should fail
	runId, err = localScoot.Run("", cmd, emptyOutputStrategy)

	// validate failure
	if runId != "" {
		t.Fatalf("test failed when running the second sleep: runId %s was returned\n", runId)
	}
	if err == nil {
		t.Fatal("test failed when running the second sleep: no error was returned")
	}
	expectedMsg := "No runners available"
	if strings.Compare(err.Error(), expectedMsg) != 0 {
		t.Fatalf("test failed when running the second sleep: expected %s, got %s\n", expectedMsg, err.Error())
	}

}

// setup the checkouter, execer, and output creator for the daemon tests
func setup(t *testing.T, numRunners int, timeout time.Duration) testEnv {

	tmpDir, err := temp.NewTempDir("/tmp", "daemonTest")
	if err != nil {
		t.Fatalf("couldn't create temp dir:%s\n", err.Error())
	}
	checkouter := snapshots.MakeTempCheckouter(tmpDir)

	exec := os.NewExecer()
	outputCreator := runners.NewNullOutputCreator()
	localScoot := NewLocalScoot(numRunners, exec, checkouter, outputCreator, timeout)
	return testEnv{execer: exec, tmpDirForCheckout: tmpDir.Dir, checkouter: checkouter, outputCreator: outputCreator, localScoot: localScoot}
}

// clean up after the test - remove the temp directory that was created by the temp checkouter
func teardown(t *testing.T, env testEnv) {

	err := sysOS.RemoveAll(env.tmpDirForCheckout)
	if err != nil {
		t.Fatalf("error removing temp directory:%s\n", env.tmpDirForCheckout)
	}

}
