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
	qr            *QueueingRunner
	maxQLen       int
}

const errorMsgFromRunner = "Error in fakeRunner Run()"

// Send a run request that pauses, then another run request.
// Verify that the status of the first run request is running, and
// the status of the second request is queued.
// Then send a signal to allow the first (paused) run request to finish.
// Wait 1ms then verify that the second run request was run.
func TestQueueing2Messages(t *testing.T) {

	testEnv := setup(false, 10, t)
	defer teardown(testEnv)

	qr := testEnv.qr

	testEnv.waitGroup.Add(1) // set the pause condition

	// send the first command - it should pause
	run1, _ := validateRunRequest("1st Run()", []string{"pause", "complete 0"}, []runner.ProcessState{runner.PREPARING}, "", qr, t)

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

	testEnv := setup(false, 4, t)
	defer teardown(testEnv)
	qr := testEnv.qr

	testEnv.waitGroup.Add(1) // set the pause condition

	var args [6][]string

	// phase1: -----------------fill up the queue and send one extra overflow request
	args[0] = []string{"pause", "complete 0"} // send the first command - it should pause
	validateRunRequest("phase1, 1st Run():", args[0], []runner.ProcessState{runner.PREPARING}, "", qr, t)
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
	waitForStatus("phase2, wait for 4th run complete:", runner.RunId("4"), []runner.ProcessState{runner.COMPLETE}, 10*time.Millisecond, qr, t)

	// phase 3: ----------- verify runids
	// validate that runids only go up to 4 (0th run started immediately, then queue held 1-4)
	validateRunIds("phase3, ", 4, qr, t)

	// phase 4: ------------refill the queue and send one extra overflow request
	testEnv.waitGroup.Add(1)
	validateRunRequest("phase4, 1st Run():", args[0], []runner.ProcessState{runner.PREPARING}, "", qr, t)
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

	waitForStatus("phase5, wait for 4th run complete:", runner.RunId("9"), []runner.ProcessState{runner.COMPLETE}, 20*time.Millisecond, qr, t)

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
	validateStatus(tag, runner.RunId(maxRunId+1), UnknownRunIdMsg, []runner.ProcessState{runner.BADREQUEST}, qr, t)
}

func TestUnknownRunIdInStatusRequest(t *testing.T) {

	testEnv := setup(false, 4, t)
	defer teardown(testEnv)
	qr := testEnv.qr

	// send the first command telling sim execer to pause
	testEnv.waitGroup.Add(1)
	var args [6][]string
	args[0] = []string{"pause", "complete 0"}
	validateRunRequest("1st Run():", args[0], []runner.ProcessState{runner.PREPARING}, "", qr, t)

	for i := 1; i < 3; i++ {
		// send a second command
		args[i] = []string{"complete " + strconv.Itoa(i)}
		tag := fmt.Sprintf("Run() %d:", i+6)
		validateRunRequest(tag, args[i], []runner.ProcessState{runner.PENDING}, "", qr, t)
	}

	validateStatus("unknown runID:", "badRunId", UnknownRunIdMsg, []runner.ProcessState{runner.BADREQUEST}, qr, t)
	testEnv.waitGroup.Done()
}

func TestRunnerReturningAnErrorOnRunRequest(t *testing.T) {

	testEnv := setup(true, 4, t)
	defer teardown(testEnv)
	qr := testEnv.qr

	validateRunRequest("Run():", []string{"Command:", "Run", "return", "error", "test"}, []runner.ProcessState{runner.BADREQUEST}, errorMsgFromRunner, qr, t)
}

func SkipTestStatusAll(t *testing.T) {

	testEnv := setup(false, 10, t)
	defer teardown(testEnv)
	qr := testEnv.qr

	testEnv.waitGroup.Add(1) // set the pause condition

	// put commands in the queue with the first command paused
	var args [6][]string

	args[0] = []string{"pause", "complete 0"} // send the first command - it should pause
	validateRunRequest("phase1, 1st Run():", args[0], []runner.ProcessState{runner.PREPARING}, "", qr, t)
	waitForStatus("phase1, wait for first run running:", runner.RunId("0"), []runner.ProcessState{runner.RUNNING}, 10*time.Millisecond, qr, t)

	for i := 1; i < 5; i++ {
		// send a second command
		args[i] = []string{"complete " + strconv.Itoa(i)}
		tag := fmt.Sprintf("phase1, Run() %d:", i)
		validateRunRequest(tag, args[i], []runner.ProcessState{runner.PENDING}, "", qr, t)
	}

	// check the status
	statuss := qr.getStatusAll()

	if len(statuss) != 5 {
		t.Fatalf("Wrong length on statusAll response: expected 5, got %d", len(statuss))
	}

	if statuss[0].State != runner.RUNNING && statuss[0].State != runner.PREPARING && statuss[0].State != runner.PENDING {
		t.Fatalf("Wrong statuss[0]: expected Running, Preparing or Pending, got %s", statuss[0].State)
	}

	for i := 1; i < 5; i++ {
		if statuss[i].State != runner.PENDING {
			t.Fatalf("Wrong statuss[%d]: expected Pending, got %s", i, statuss[i].State)
		}
	}
	testEnv.waitGroup.Done()

}

func SkipTestAbortTop2ReuqestsWhenPaused(t *testing.T) {
	testEnv := setup(false, 10, t)
	defer teardown(testEnv)
	qr := testEnv.qr

	testEnv.waitGroup.Add(1) // set the pause condition

	// put commands in the queue with the first command paused
	var args [6][]string

	args[0] = []string{"pause", "complete 0"} // send the first command - it should pause
	validateRunRequest("phase1, 1st Run():", args[0], []runner.ProcessState{runner.PREPARING}, "", qr, t)
	waitForStatus("phase1, wait for first run running:", runner.RunId("0"), []runner.ProcessState{runner.RUNNING}, 10*time.Millisecond, qr, t)

	for i := 1; i < 5; i++ {
		// send a second command
		args[i] = []string{"complete " + strconv.Itoa(i)}
		tag := fmt.Sprintf("phase1, Run() %d:", i)
		validateRunRequest(tag, args[i], []runner.ProcessState{runner.PENDING}, "", qr, t)
	}

	for i := 0; i < 2; i++ {
		s, err := qr.Abort(runner.RunId(strconv.Itoa(i)))

		if err != nil {
			t.Fatalf("Error aborting run(%d):'%s'", i, err.Error())
		}

		if s.State != runner.ABORTED {
			t.Fatalf("Aborted run(%d) did not return 'Aborted' state, got: '%s'", i, s.State)
		}

		s, err = qr.Status(runner.RunId(strconv.Itoa(i)))
		if err != nil {
			t.Fatalf("Error getting status of run(%d):'%s'", i, err.Error())
		}
		if s.State != runner.ABORTED {
			t.Fatalf("Error getting status of run(%d) expected state 'Aborted', got '%s'", i, s.State)
		}

	}

	testEnv.waitGroup.Done()

	sAll, err := qr.StatusAll()

	if err != nil {
		t.Fatalf("Error getting status All '%s'", err.Error())
	}

	if sAll[0].State != runner.ABORTED {
		t.Fatalf("Error, statusAll state for run(0), expected 'Aborted', got '%s'", sAll[0].State)
	}
	if sAll[1].State != runner.ABORTED {
		t.Fatalf("Error, statusAll state for run(1), expected 'Aborted', got '%s'", sAll[1].State)
	}
	for i := 2; i < 5; i++ {
		if sAll[i].State == runner.ABORTED {
			t.Fatalf("Error, statusAll state for run(%d), expected anything but 'Aborted', got 'Aborted'", i)
		}
	}
}

func SkipTestAbortingFirst3RequestsInQueue(t *testing.T) {
	testEnv := setup(false, 10, t)
	defer teardown(testEnv)
	qr := testEnv.qr

	testEnv.waitGroup.Add(1) // set the pause condition

	// put commands in the queue with the first command paused
	var args [6][]string

	args[0] = []string{"pause", "complete 0"} // send the first command - it should pause
	validateRunRequest("phase1, 1st Run():", args[0], []runner.ProcessState{runner.PREPARING}, "", qr, t)
	waitForStatus("phase1, wait for first run running:", runner.RunId("0"), []runner.ProcessState{runner.RUNNING}, 10*time.Millisecond, qr, t)

	for i := 1; i < 5; i++ {
		// send a second command
		args[i] = []string{"complete " + strconv.Itoa(i)}
		tag := fmt.Sprintf("phase1, Run() %d:", i)
		validateRunRequest(tag, args[i], []runner.ProcessState{runner.PENDING}, "", qr, t)
	}

	for i := 0; i < 3; i++ {
		s, err := qr.Abort(runner.RunId(strconv.Itoa(i)))

		if err != nil {
			t.Fatalf("Error aborting run (%d), %s", i, err.Error())
		}
		if s.State != runner.ABORTED {
			t.Fatalf("Error expected Aborted state for run(%d), got '%s'", i, s.State)
		}
	}

	// let the rest of the run requests run
	testEnv.waitGroup.Done()

	sAll, err := qr.StatusAll()

	if err != nil {
		t.Fatalf("Error getting status All '%s'", err.Error())
	}

	for i := 3; i < 5; i++ {
		if sAll[i].State == runner.ABORTED {
			t.Fatalf("Error, statusAll state for run(%d), expected anything but 'Aborted', got 'Aborted'", i)
		}
	}

}

func validateRunRequest(tag string, args []string, expectedStates []runner.ProcessState, expectedErrorClause string, qr *QueueingRunner, t *testing.T) (runner.ProcessStatus, error) {
	cmd := &runner.Command{Argv: args[:], Timeout: 30 * time.Minute}
	status, err := qr.Run(cmd)
	if err != nil {
		t.Fatalf("%s:Run request %v. Expected nil error,  got error: '%s'\n", tag, args, err.Error())
	}
	if strings.Compare(expectedErrorClause, status.Error) != 0 {
		t.Fatalf("%s: Run request %v. Expected error:'%s', got %s\n", tag, args, expectedErrorClause, status.Error)
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
	if err != nil {
		t.Fatalf("%s:Run request %s. Expected  nil error,  got error: '%s'\n", tag, string(runId), err.Error())
	}
	if strings.Compare(expectedErrorClause, status.Error) != 0 {
		t.Fatalf("%s: Run request %s. Expected error:'%s', got %s\n", tag, string(runId), expectedErrorClause, status.Error)
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

func setup(useFakeRunner bool, qLen int, t *testing.T) *testEnv {

	runnerAvailableCh := make(chan struct{})

	ctx := context.TODO()

	var runner runner.Runner
	var wg *sync.WaitGroup
	if useFakeRunner {
		runner = getFakeRunner()
	} else {
		runner, wg = makeRunnerWithSimExecer(runnerAvailableCh, t)
	}

	qr := NewQueuingRunner(ctx, runner, qLen, runnerAvailableCh).(*QueueingRunner)

	return &testEnv{commandRunner: runner, waitGroup: wg, qr: qr, maxQLen: qLen}

}

func makeRunnerWithSimExecer(runnerAvailableCh chan struct{}, t *testing.T) (runner.Runner, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	ex := execers.NewSimExecer(wg)
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Test setup() failed getting temp dir:%s", err.Error())
	}

	outputCreator, err := NewOutputCreator(tempDir)
	if err != nil {
		t.Fatalf("Test setup() failed getting output creator:%s", err.Error())
	}
	r := NewSimpleReportBackRunner(ex, snapshots.MakeInvalidCheckouter(), outputCreator, runnerAvailableCh)
	return r, wg
}

func teardown(testEnv *testEnv) {

}

type fakeRunner struct{}

func (er *fakeRunner) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	return runner.ProcessStatus{State: runner.BADREQUEST}, fmt.Errorf(errorMsgFromRunner)
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
