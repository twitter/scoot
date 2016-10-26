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

	testEnv.waitGroup.Add(1) // set the pause condition

	// send the first command - it should pause
	run1, _ := validateRunRequest("1st Run()", []string{"pause", "complete 0"}, []runner.ProcessState{runner.PENDING}, "", qr, t)

	// send a second command
	run2, _ := validateRunRequest("2nd Run()", []string{"complete 1"}, []runner.ProcessState{runner.PENDING}, "", qr, t)

	// get the status of the first command - it should be running or preparing
	waitForStatus("1st run running:", run1.RunId, []runner.ProcessState{runner.RUNNING}, 10*time.Millisecond, qr, t)
	//expectedStates1 := []runner.ProcessState{runner.RUNNING, runner.PREPARING}
	//validateStatus("1st Status():", run1.RunId, "", expectedStates1, qr, t)

	// get the status of the second command - it should be pending
	validateStatus("2nd Status() when blocked:", run2.RunId, "", []runner.ProcessState{runner.PENDING}, qr, t)

	testEnv.waitGroup.Done() // send signal to end first (paused) request

	waitForStatus("2nd run complete:", run2.RunId, []runner.ProcessState{runner.COMPLETE}, 10*time.Millisecond, qr, t)
}

// this test has the following phases:
// 1. fill up the queue and send one 'overflow' request (the first run pauses)
// 2. unpause the first run and let the queue empty
// 3. verify that runIds only go to 4
// 4. fill up the queue again with one 'overflow' request (again with first command paused)
// 5. unpause the first command from 3 and let the queued empty again
// 6. verify that the runIds now go to 9
func TestQueueingMoreThanMaxMessage(t *testing.T) {
	runnerAvailableCh := make(chan struct{})
	testEnv, err := setup(runnerAvailableCh, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer teardown(testEnv)

	ctx := context.TODO()

	qr := NewQueuingRunner(ctx, testEnv.commandRunner, 4, runnerAvailableCh).(*QueueingRunner)

	testEnv.waitGroup.Add(1) // set the pause condition

	var args [6][]string

	// phase1: -----------------fill up the queue and send one extra overflow request
	args[0] = []string{"pause", "complete 0"} // send the first command - it should pause
	validateRunRequest("phase1, 1st Run():", args[0], []runner.ProcessState{runner.PENDING}, "", qr, t)
	waitForStatus("phase1, wait for first run running:", runner.RunId("0"), []runner.ProcessState{runner.RUNNING}, 10*time.Millisecond, qr, t)

	// send commands to fill up the queue
	for i := 1; i < 5; i++ {
		// send a second command
		args[i] = []string{"complete " + strconv.Itoa(i)}
		tag := fmt.Sprintf("phase1, Run() %d:", i)
		validateRunRequest(tag, args[i], []runner.ProcessState{runner.PENDING}, "", qr, t)
	}

	// wait for the requests to be placed on the queue
	waitForStatus("phase1, wait for 4th run pending:", runner.RunId("4"), []runner.ProcessState{runner.PENDING}, 10*time.Millisecond, qr, t)

	// validate other runs are all pending
	for i := 1; i < 4; i++ {
		tag := fmt.Sprintf("phase1, Status() %d when blocked:", i)
		validateStatus(tag, runner.RunId(strconv.Itoa(i)), "", []runner.ProcessState{runner.PENDING}, qr, t)

	}

	// send command that can't fit on the queue
	args[5] = []string{"complete 5"}
	validateRunRequest("phase1, overflow request:", args[5], []runner.ProcessState{runner.FAILED}, QueueFullMsg, qr, t)

	// phase2: -----------unpause the first run and let the queue empty
	testEnv.waitGroup.Done() // unpause the first command so the queue will be emptied

	// wait for the first run to complete
	waitForStatus("phase2, wait for 1st run complete:", runner.RunId("0"), []runner.ProcessState{runner.COMPLETE}, 2*time.Millisecond, qr, t)
	// wait for the requests to be placed on the queue
	waitForStatus("phase2, wiat for 4th run complete:", runner.RunId("4"), []runner.ProcessState{runner.COMPLETE}, 10*time.Millisecond, qr, t)

	// phase 3: ----------- verify runids
	// validate that runids only go up to 4 (0th run started immediately, then queue held 1-4)
	validateRunIds("phase3, ", 4, qr, t)

	// phase 4: ------------refill the queue and send one extra overflow request
	testEnv.waitGroup.Add(1)
	validateRunRequest("phase4, 1st Run():", args[0], []runner.ProcessState{runner.PENDING}, "", qr, t)
	waitForStatus("phase4, wait for 1st run running:", runner.RunId("5"), []runner.ProcessState{runner.RUNNING}, 2*time.Millisecond, qr, t)

	for i := 1; i < 5; i++ {
		tag := fmt.Sprintf("phase4, Run() %d:", i+6)
		validateRunRequest(tag, args[i], []runner.ProcessState{runner.PENDING}, "", qr, t)
	}

	// send command that can't fit on the queue
	args[5] = []string{"complete 5"}
	validateRunRequest("phase4, overflow request", args[5], []runner.ProcessState{runner.FAILED}, QueueFullMsg, qr, t)

	// phase 5: ------------- unpause and let the queue empty again
	testEnv.waitGroup.Done() // let the paused run continue

	// wait for the 5th run (first run in second set) to complete
	waitForStatus("phase5, wait for 1st run complete:", runner.RunId("5"), []runner.ProcessState{runner.COMPLETE}, 2*time.Millisecond, qr, t)

	waitForStatus("phase5, wait for 4th run complete:", runner.RunId("9"), []runner.ProcessState{runner.COMPLETE}, 10*time.Millisecond, qr, t)

	// phase 6: ------------- validate that the runids only go to 9
	validateRunIds("phase 6", 9, qr, t)
}

// validate that the run ids only go up to maxRunId and that they are all completed
func validateRunIds(tagPrefix string, maxRunId int, qr *QueueingRunner, t *testing.T) {
	for i := 0; i <= maxRunId; i++ {
		tag := fmt.Sprintf("%s Status() %d, runid check", tagPrefix, i)
		validateStatus(tag, runner.RunId(strconv.Itoa(i)), "", []runner.ProcessState{runner.COMPLETE}, qr, t)
	}
	tag := fmt.Sprintf("%s RunId %d should not exist", tagPrefix, maxRunId+1)
	validateStatus(tag, runner.RunId(maxRunId+1), "", []runner.ProcessState{runner.BADREQUEST}, qr, t)
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
	validateRunRequest("1st Run():", args[0], []runner.ProcessState{runner.PENDING}, "", qr, t)

	for i := 1; i < 3; i++ {
		// send a second command
		args[i] = []string{"complete " + strconv.Itoa(i)}
		tag := fmt.Sprintf("Run() %d:", i+6)
		validateRunRequest(tag, args[i], []runner.ProcessState{runner.PENDING}, "", qr, t)
	}

	expectedStatus := []runner.ProcessState{runner.BADREQUEST}
	processStatus := validateStatus("unknown runID:", "badRunId", "", expectedStatus, qr, t)
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
	status, _ := validateRunRequest("Run():", []string{"Command:", "Run", "return", "error", "test"}, []runner.ProcessState{runner.PENDING}, "", qr, t)
	time.Sleep(2 * time.Millisecond)
	validateStatus("Status():", status.RunId, errorMsgFromRunner, []runner.ProcessState{runner.BADREQUEST}, qr, t)
}

func validateRunRequest(tag string, args []string, expectedStates []runner.ProcessState, expectedErrorClause string, qr *QueueingRunner, t *testing.T) (runner.ProcessStatus, error) {
	cmd := &runner.Command{Argv: args[:], Timeout: 30 * time.Second}
	status, err := qr.Run(cmd)
	if err != nil && !strings.Contains(err.Error(), expectedErrorClause) {
		t.Fatalf("%s:Run request %v. Expected error:'%s', got error: '%s'\n", tag, args, expectedErrorClause, err.Error())
	}
	if strings.Compare(expectedErrorClause, "") != 0 && err == nil {
		t.Fatalf("%s: Run request %v. Expected error:'%s', got nil\n", tag, args, expectedErrorClause)
	}
	if len(expectedStates) == 0 {
		return status, err // don't check the state
	}

	if doesMatchState(expectedStates, status) {
		return status, err
	}

	t.Fatalf("%s: Run id:%s, Expected %v got %s", tag, string(status.RunId), expectedStates, status.State.String())

	return status, err
}

func validateStatus(tag string, runId runner.RunId, expectedErrorClause string, expectedStates []runner.ProcessState, qr *QueueingRunner, t *testing.T) runner.ProcessStatus {
	status, err := qr.Status(runId)
	if err != nil && !strings.Contains(err.Error(), expectedErrorClause) {
		t.Fatalf("%s:Status request error for runId:'%s'. expected nil got: '%s'\n", tag, string(runId), err.Error())
	}

	if len(expectedStates) == 0 {
		return status // don't check the state
	}
	if doesMatchState(expectedStates, status) {
		return status
	}

	t.Fatalf("%s: Run '%s' state error: expected one of: %v, got: '%s'\n", tag, runId, expectedStates, status.State.String())

	return runner.ProcessStatus{}
}

func doesMatchState(statesToMatch []runner.ProcessState, status runner.ProcessStatus) bool {
	for _, state := range statesToMatch {
		if status.State == state {
			return true
		}
	}

	return false
}

//TODO - use poll when it becomes available
func waitForStatus(tag string, runId runner.RunId, allowedStates []runner.ProcessState, maxWait time.Duration, qr *QueueingRunner, t *testing.T) (runner.ProcessStatus, error) {
	startWait := time.Now()

	for {
		status, err := qr.Status(runId)
		if err != nil {
			t.Fatalf("%s got error:%s, expected nil", tag, err.Error())
		}
		if doesMatchState(allowedStates, status) {
			return status, nil
		}
		now := time.Now()
		elapsedTime := time.Duration(now.Nanosecond() - startWait.Nanosecond())
		if elapsedTime.Nanoseconds() > maxWait.Nanoseconds() || elapsedTime.Nanoseconds() < 0 {
			t.Fatalf("%s wait expired, got state:%s when looking for %v", tag, status.State, allowedStates)
		}
		time.Sleep(100 * time.Nanosecond)
	}
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
