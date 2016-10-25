package local

import (
	"context"
	"fmt"
	"strconv"
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

const errorMsgFromRunner = "Error in fakeRunner Run()"

// Send a run request that pauses, then another run request.
// Verify that the status of the first run request is running, and
// the status of the second request is queued.
// Then send a signal to allow the first (paused) run request to finish.
// Wait 1ms then verify that the second run request was run.
func TestQueueing2Messages(t *testing.T) {

	runnerAvailableCh := make(chan struct{})
	testEnv, err := setup(runnerAvailableCh, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer teardown(testEnv)

	ctx := context.TODO()

	qr := NewQueuingRunner(ctx, testEnv.commandRunner, 10, runnerAvailableCh).(*QueueingRunner)

	// send the first command telling sim execer to pause
	testEnv.waitGroup.Add(1)
	args1 := []string{"pause", "complete 0"}
	run1, _ := sendRunRequest(args1, "queued", qr, t)

	// send a second command
	args2 := []string{"complete 1"}
	run2, _ := sendRunRequest(args2, "queued", qr, t)

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
}

func TestQueueingMoreThanMaxMessage(t *testing.T) {
	runnerAvailableCh := make(chan struct{})
	testEnv, err := setup(runnerAvailableCh, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer teardown(testEnv)

	ctx := context.TODO()

	qr := NewQueuingRunner(ctx, testEnv.commandRunner, 4, runnerAvailableCh).(*QueueingRunner)

	// send the first command telling sim execer to pause
	testEnv.waitGroup.Add(1)
	var args [6][]string
	args[0] = []string{"pause", "complete 0"}
	sendRunRequest(args[0], "", qr, t)

	for i := 1; i < 5; i++ {
		// send a second command
		args[i] = []string{"complete " + strconv.Itoa(i)}
		sendRunRequest(args[i], "", qr, t)
	}

	args[5] = []string{"complete 5"}
	sendRunRequest(args[5], QueueFullMsg, qr, t)

}

func TestUnknownRunIdInStatusRequest(t *testing.T) {
	runnerAvailableCh := make(chan struct{})
	testEnv, err := setup(runnerAvailableCh, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer teardown(testEnv)

	ctx := context.TODO()

	qr := NewQueuingRunner(ctx, testEnv.commandRunner, 4, runnerAvailableCh).(*QueueingRunner)

	// send the first command telling sim execer to pause
	testEnv.waitGroup.Add(1)
	var args [6][]string
	args[0] = []string{"pause", "complete 0"}
	sendRunRequest(args[0], "", qr, t)

	for i := 1; i < 3; i++ {
		// send a second command
		args[i] = []string{"complete " + strconv.Itoa(i)}
		sendRunRequest(args[i], "", qr, t)
	}

	expectedStatus := []runner.ProcessState{runner.BADREQUEST}
	processStatus := validateStatus("badRunId", "", expectedStatus, qr, t)
	if strings.Compare(UnknownRunIdMsg, processStatus.Error) != 0 || strings.Compare("badRunId", string(processStatus.RunId)) != 0 {
		t.Fatalf("Expected '%s' message or '%s' runid, got '%s' message and '%s' runid", UnknownRunIdMsg, "badRunId", processStatus.Error, string(processStatus.RunId))
	}
}

func TestRunnerReturningAnErrorOnRunRequest(t *testing.T) {
	testEnv, err := setup(nil, true)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer teardown(testEnv)

	ctx := context.TODO()

	qr := NewQueuingRunner(ctx, testEnv.commandRunner, 4, nil).(*QueueingRunner)

	// send the first command telling sim execer to pause
	var args [6][]string
	args[0] = []string{"Run", "return", "error", "test"}
	status, _ := sendRunRequest(args[0], errorMsgFromRunner, qr, t)
	time.Sleep(2 * time.Millisecond)
	status = validateStatus(status.RunId, "", []runner.ProcessState{runner.BADREQUEST}, qr, t)
	if status.State != runner.BADREQUEST || strings.Compare(errorMsgFromRunner, status.Error) != 0 {
		t.Fatalf("TestRunRequestReturningAnError: expected '%s' and '%s', got '%s' and '%s'", runner.BADREQUEST.String(), errorMsgFromRunner, status.State.String(), status.Error)
	}
}

func sendRunRequest(args []string, expectedErrorClause string, qr *QueueingRunner, t *testing.T) (runner.ProcessStatus, error) {
	cmd := &runner.Command{Argv: args[:], Timeout: 3 * time.Second}
	status, err := qr.Run(cmd)
	if err != nil && !strings.Contains(err.Error(), expectedErrorClause) {
		t.Fatalf("Run request %v. Expected error:%s, got error: %s\n", args, expectedErrorClause, err.Error())
	}

	return status, err
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

func setup(runnerAvailableCh chan struct{}, useFakeRunner bool) (*testEnv, error) {

	if useFakeRunner {
		return &testEnv{commandRunner: getFakeRunner()}, nil
	}

	var runner runner.Runner
	var wg *sync.WaitGroup
	runner, wg, err := makeRunnerWithSimExecer(runnerAvailableCh)
	if err != nil {
		return &testEnv{commandRunner: runner, waitGroup: wg}, err
	}

	return &testEnv{commandRunner: runner, waitGroup: wg}, nil

}

func makeRunnerWithSimExecer(runnerAvailableCh chan struct{}) (runner.Runner, *sync.WaitGroup, error) {
	wg := &sync.WaitGroup{}
	ex := execers.NewSimExecer(wg)
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		return nil, wg, err
	}

	outputCreator, err := NewOutputCreator(tempDir)
	if err != nil {
		return nil, wg, err
	}
	r := NewSimpleReportBackRunner(ex, snapshots.MakeInvalidCheckouter(), outputCreator, runnerAvailableCh)
	return r, wg, nil
}

func teardown(testEnv *testEnv) {

}

type fakeRunner struct{}

func (er *fakeRunner) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	return runner.ProcessStatus{}, fmt.Errorf(errorMsgFromRunner)
}

// Status checks the status of run.
func (er *fakeRunner) Status(run runner.RunId) (runner.ProcessStatus, error) {
	return runner.ProcessStatus{}, nil
}

// Current status of all runs, running and finished, excepting any Erase()'s runs.
func (er *fakeRunner) StatusAll() ([]runner.ProcessStatus, error) {
	return []runner.ProcessStatus{runner.ProcessStatus{}}, nil
}

// Kill the given run.
func (er *fakeRunner) Abort(run runner.RunId) (runner.ProcessStatus, error) {
	return runner.ProcessStatus{}, nil
}

// Prunes the run history so StatusAll() can return a reasonable number of runs.
func (er *fakeRunner) Erase(run runner.RunId) error {
	return nil
}

func getFakeRunner() runner.Runner {
	return &fakeRunner{}
}
