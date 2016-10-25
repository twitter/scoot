package local

import (
	"context"
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

// Send a run request that pauses, then another run request.
// Verify that the status of the first run request is running, and
// the status of the second request is queued.
// Then send a signal to allow the first (paused) run request to finish.
// Wait 1ms then verify that the second run request was run.
func TestQueueing2Messages(t *testing.T) {

	runnerAvailableCh := make(chan struct{})
	testEnv, err := setup(runnerAvailableCh)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer teardown(testEnv)

	ctx := context.TODO()

	qr := NewQueuingRunner(ctx, testEnv.commandRunner, 10, runnerAvailableCh).(*QueueingRunner)

	// send the first command telling sim execer to pause
	testEnv.waitGroup.Add(1)
	args1 := []string{"pause", "complete 0"}
	run1 := sendRunRequest(args1, "queued", qr, t)

	// send a second command
	args2 := []string{"complete 1"}
	run2 := sendRunRequest(args2, "queued", qr, t)

	// get the status of the first command - it should be running or preparing
	expectedStates1 := []runner.ProcessState{runner.RUNNING, runner.PREPARING}
	validateStatus(run1.RunId, "", expectedStates1, qr, t)

	// get the status of the second command - it should be pending
	expectedStates2 := []runner.ProcessState{runner.PENDING}
	validateStatus(run2.RunId, "", expectedStates2, qr, t)

	testEnv.waitGroup.Done() // send signal to end first (paused) request

	// TODO - why do we have to pause to allow run2 to start?
	time.Sleep(1 * time.Millisecond)
	status := validateStatus(run2.RunId, "", nil, qr, t)
	if status.State == runner.PENDING {
		t.Fatal("Run2 should have started, state is still pending")
	}
	// alternative to the time.Sleep(1ms):
	// loop for up to 1ms or until run2 is done
	//loopStart := time.Now()
	//maxLoopTime := 1 * time.Millisecond
	//for {
	//	// get the status of the second request
	//	status, err := qr.Status(run2.RunId)
	//	if err != nil {
	//		t.Fatalf("Run2 status error: got error %s\n", err.Error())
	//	}
	//	if status.State.IsDone() {
	//		break
	//	}
	//
	//	elapsedTime := time.Since(loopStart)
	//	log.Printf("******elapsed time:%d, maxTime:%d", elapsedTime.Nanoseconds(), maxLoopTime.Nanoseconds())
	//	if elapsedTime.Nanoseconds() > maxLoopTime.Nanoseconds() {
	//		t.Fatalf("Run2 did not complete withing 1ms")
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//
	//}

}

func sendRunRequest(args []string, expectedErrorClause string, qr *QueueingRunner, t *testing.T) runner.ProcessStatus {
	cmd := &runner.Command{Argv: args[:], Timeout: 3 * time.Second}
	status, err := qr.Run(cmd)
	if err != nil && !strings.Contains(err.Error(), expectedErrorClause) {
		t.Fatalf("Run request %v got error: %s\n", args, err.Error())
	}

	return status
}

func validateStatus(runId runner.RunId, expectedErrorClause string, expectedStates []runner.ProcessState, qr *QueueingRunner, t *testing.T) runner.ProcessStatus {
	status, err := qr.Status(runId)
	if err != nil && !strings.Contains(err.Error(), expectedErrorClause) {
		t.Fatalf("Status request error for runId:%s. expected nil got: %s\n", string(runId), err.Error())
	}

	if len(expectedStates) == 0 {
		return status // don't check the state
	}
	for _, allowedState := range expectedStates {
		if status.State == allowedState {
			return status
		}
	}

	t.Fatalf("Run %s state error: expected one of: %v, got: %s\n", runId, expectedStates, status.State.String())

	return runner.ProcessStatus{}
}

func setup(runnerAvailableCh chan struct{}) (*testEnv, error) {

	var runner runner.Runner
	var wg *sync.WaitGroup
	runner, wg = makeRunnerWithSimExecer(runnerAvailableCh)

	return &testEnv{commandRunner: runner, waitGroup: wg}, nil

}

func makeRunnerWithSimExecer(runnerAvailableCh chan struct{}) (runner.Runner, *sync.WaitGroup) {
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
	r := NewSimpleReportBackRunner(ex, snapshots.MakeInvalidCheckouter(), outputCreator, runnerAvailableCh)
	return r, wg
}

func teardown(testEnv *testEnv) {

}
