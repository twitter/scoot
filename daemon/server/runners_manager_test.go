package server

import (
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/os"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/snapshots"

	"strings"
	"testing"
	"log"
)

func TestNoRunners(t *testing.T) {

	var exec execer.Execer = os.NewExecer()
	var noopCheckouter snapshot.Checkouter = snapshots.MakeNoOpCheckouter()
	var outputCreator runner.OutputCreator = runners.NewNullOutputCreator()
	runnersManager := NewRunnerManager(0, exec, noopCheckouter, outputCreator)

	cmd := []string{"echo", "hello world"}
	var emptyOutputStrategy OutputStrategy = ""
	_, err := runnersManager.Run("1", cmd, emptyOutputStrategy)
	if err == nil {
		t.Fatalf("No runners test failed")
	}
}

func TestAllRunnersBusyProcessing(t *testing.T) {

	var exec execer.Execer = os.NewExecer()

	var noopCheckouter snapshot.Checkouter = snapshots.MakeNoOpCheckouter()
	var outputCreator runner.OutputCreator = runners.NewNullOutputCreator()
	runnersManager := NewRunnerManager(1, exec, noopCheckouter, outputCreator)

	// start a runner - it should start successfully
	cmd := []string{"sleep", "1"}
	var emptyOutputStrategy OutputStrategy = ""
	runId, err := runnersManager.Run("", cmd, emptyOutputStrategy)
	// validate successful start
	if runId == "" {
		t.Fatal("failed running the first sleep: no runId returned")
	}
	if err != nil {
		t.Fatalf("error returned on the first sleep: %s\n", err.Error())
	}

	// start the second runner - it should fail
	runId, err = runnersManager.Run("", cmd, emptyOutputStrategy)

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
