package local

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

type testEnv struct {
	commandRunner runner.Runner
	waitGroup     *sync.WaitGroup
}

// Send a pausing run request, then another run request.  The status of the first
// run request should be running, the status of the second request should be queued.
// Then send a signal to allow the first (paused) run request to finish.
// Wait a short time, and verify that the second run request was run.
func TestQueueingAMessage(t *testing.T) {

	testEnv, err := setup()
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer teardown(testEnv)

	ctx := context.TODO()

	qr := NewQueuingRunner(ctx, testEnv.commandRunner, 10)

	// send the first command telling sim execer to pause
	timeout := 3 * time.Second
	args := []string{"pause", "complete 0"}
	testEnv.waitGroup.Add(1)
	cmd := runner.Command{Argv: args[:], Timeout: timeout}
	run1, err := qr.Run(&cmd)
	if err != nil  && !strings.Contains(err.Error(), "queued") {
		t.Fatalf("Run1 request got error: %s\n", err.Error())
	}

	// send a second command
	args = []string{"complete 1"}
	cmd = runner.Command{Argv: args[:], Timeout: timeout}
	run2, err := qr.Run(&cmd)
	if err != nil  && !strings.Contains(err.Error(), "queued") {
		t.Fatalf("Run2 , got error: %s\n", err.Error())
	}

	// pause giving time for the first request to be pulled off the queue and started
	// TODO - discuss this with team - is there a better way?
	time.Sleep(1000 * time.Millisecond)
	// get the status - it should be running
	status, err := qr.Status(run1.RunId)
	if err != nil {
		t.Fatalf("Run1 status error: expected nil got: %s\n", err.Error())
	}
	if status.State != runner.RUNNING {
		t.Fatalf("Run1 state error: expected: RUNNING, got: %s\n", status.State.String())
	}

	// get the status of the second request - it should be an error message indicating it is queued
	status, err = qr.Status(run2.RunId)
	if err == nil {
		t.Fatalf("Run2 status error: expected queued message got nil\n")
	}
	expectedMsg := fmt.Sprintf(QueuedMsg, run2.RunId)
	if strings.Compare(expectedMsg, err.Error()) != 0 {
		t.Fatalf("Run2 error: expected: %s, got: %s\n", expectedMsg, err.Error())
	}

	// send signal to end first (paused) request
	testEnv.waitGroup.Done()
	// wait 0.5 second for the first request to finish and second command to be pulled off the queue and started
	// TODO - discuss this with team - is there a better way?
	time.Sleep(750 * time.Millisecond)

	// get the status of the second request
	status, err = qr.Status(run2.RunId)
	if err != nil {
		t.Fatalf("Run2 status error: got error %s\n", err.Error())
	}
	// the second request should have been able to complete during the prior sleep
	if !status.State.IsDone() {
		t.Fatalf("Run2 error: expected state to be done, got: %s\n", status.State.String())
	}

}

func setup() (*testEnv, error) {

	var runner runner.Runner
	var wg *sync.WaitGroup
	runner, wg = makeRunnerWithSimExecer()

	return &testEnv{commandRunner: runner, waitGroup: wg}, nil

}

func makeRunnerWithSimExecer() (runner.Runner, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	ex := execers.NewSimExecer(wg)
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		panic(err)
	}

	outputCreator, err := NewOutputCreator(tempDir)
	if err != nil {
		panic(err)
	}
	r := NewSimpleRunner(ex, snapshots.MakeInvalidCheckouter(), outputCreator)
	return r, wg
}

func teardown(testEnv *testEnv) {

}
