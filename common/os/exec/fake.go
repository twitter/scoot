package exec

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
)

type (
	// ValidatingExecer is an OsExec implementation that instead of running Commands,
	// validates that commands would have been run against an expected set.
	// NOTE that ValidatingExecer currently does not support overrides of calls to an underlying
	// Cmd.Process or Cmd.ProcessState. Client code that attempts to access these values will panic.
	ValidatingExecer struct {
		vCmd *ValidatingCmd
	}

	// ValidatingCmd implements Cmd. It does not actually run commands, but overrides
	// run methods such as Run(), Start() and Wait() to set internal state based on
	// what would have run, so that this can be validated against expected execs.
	ValidatingCmd struct {
		Cmd
		t                 *testing.T
		currentCmd        []string
		expectedCmdsRe    [][]string
		commandIdx        int
		fakeActions       map[int]func(cmd Cmd) error
		fakeProcessStates map[int]*os.ProcessState
		fakeOutput        map[int][]byte
		doneCh            chan error
	}
)

// NewValidatingExecer returns a ValidatingExecer with a set of expected commands that will be called.
func NewValidatingExecer(t *testing.T, expectedCmdsRe [][]string) *ValidatingExecer {
	return &ValidatingExecer{vCmd: &ValidatingCmd{t: t, expectedCmdsRe: expectedCmdsRe, commandIdx: -1}}
}

// SetFakeActions allow the test to inject fake actions.  The actions map to the expected command index.
// Actions are only performed if initial command validation against expected passes.
// Actions will override the return from Run() or Wait(), setting RunResult.Error.
func (v *ValidatingExecer) SetFakeActions(fakeActions map[int]func(cmd Cmd) error) *ValidatingExecer {
	v.vCmd.fakeActions = fakeActions
	return v
}

// SetProcessStates allow the test to inject fake ProcessStates.  The ProcessStates map to the expected command index
func (v *ValidatingExecer) SetProcessStates(processStates map[int]*os.ProcessState) *ValidatingExecer {
	v.vCmd.fakeProcessStates = processStates
	return v
}

// GetStdout provide a visibility to the stdout writer so tests can inject actions that write
// test values to stdout
func (v ValidatingCmd) GetStdout() io.Writer {
	t, ok := v.Cmd.(*cmdAdapter)
	if !ok {
		log.Fatalf("v.Cmd is %T not *cmdAdapter.  The test is setup incorrectly", t)
	}
	return t.cmd.Stdout
}

// GetStderr provide a visibility to the stderr writer so tests can inject actions that write
// test values to stderr
func (v ValidatingCmd) GetStderr() io.Writer {
	t, ok := v.Cmd.(*cmdAdapter)
	if !ok {
		log.Fatalf("v.Cmd is %T not *cmdAdapter.  The test is setup incorrectly", t)
	}
	return t.cmd.Stderr
}

// Command initializes a ValidatingExecer's Cmd object. When run, will be validated
// such that command was one of the predefined expected commands.
func (v *ValidatingExecer) Command(cmd string, args ...string) Cmd {
	// Create a real Command mainly for interface compatibility
	v.vCmd.Cmd = NewOsExec().Command(cmd, args...)

	v.vCmd.currentCmd = []string{cmd}
	v.vCmd.currentCmd = append(v.vCmd.currentCmd, args...)
	v.vCmd.doneCh = make(chan error)
	return v.vCmd
}

// run validates an exec command by comparing it with the next expected one, and executes any fake actions
func (v *ValidatingCmd) run() error {
	v.commandIdx++
	err := v.validateCmd()
	if err != nil {
		log.Error(err)
		v.doneCh <- err
		return err
	}
	if v.fakeActions != nil {
		if fn, ok := v.fakeActions[v.commandIdx]; ok {
			err = fn(v)
		}
	}
	v.doneCh <- err
	return err
}

// Start overrides Start() with validating behavior.
func (v *ValidatingCmd) Start() error {
	go v.run()
	return nil
}

// Wait overrides Wait() to return immediately.
func (v *ValidatingCmd) Wait() error {
	return <-v.doneCh
}

// Run overrides Run() with validating behavior.
func (v *ValidatingCmd) Run() error {
	v.Start()
	return v.Wait()
}

func (v *ValidatingCmd) Output() ([]byte, error) {
	var outBuf bytes.Buffer
	v.SetStdout(&outBuf)
	go v.run()
	<-v.doneCh
	return outBuf.Bytes(), nil
}

// ProcessState override ProcessState() to return the injected ProcessState
func (v *ValidatingCmd) ProcessState() *os.ProcessState {
	if t, ok := v.fakeProcessStates[v.commandIdx]; ok {
		return t
	}
	return nil
}

func (v *ValidatingCmd) validateCmd() error {
	if v.commandIdx >= len(v.expectedCmdsRe) {
		return fmt.Errorf("command validation failed.\n\tonly expected %d commands.\n\treceived extra command: %s\n",
			len(v.expectedCmdsRe), v.currentCmd)
	}

	commandRes := v.expectedCmdsRe[v.commandIdx]
	if len(commandRes) != len(v.currentCmd) {
		var msg string
		for i, re := range commandRes {
			if i >= len(v.currentCmd) {
				msg = fmt.Sprintf("%s\nexpected arg %d, %s was not in the command", msg, i, re)
			} else {
				rec := regexp.MustCompile(re)
				if !rec.Match([]byte(v.currentCmd[i])) {
					msg = fmt.Sprintf("%s\nexpected arg %d: %s, got %s", msg, i, re, v.currentCmd[i])
					break
				}
			}
		}
		return fmt.Errorf("command validation failed.\n\tcmd index: %d\n\texpected: %d args (%s)\n\treceived: %d args (%s):\n%s\n",
			v.commandIdx, len(commandRes), strings.Join(commandRes, ","), len(v.currentCmd), strings.Join(v.currentCmd, ","), msg)
	}
	for i, re := range commandRes {
		rec := regexp.MustCompile(re)
		if !rec.Match([]byte(v.currentCmd[i])) {
			return fmt.Errorf("command validation failed.\n\tcmd index: %d, entry: %d\n\texpected: %s\n\treceived: %s\nexpectedCmd: %s\n\treceivedCmd: %s\n",
				v.commandIdx, i, re, v.currentCmd[i], strings.Join(commandRes, ","), strings.Join(v.currentCmd, ","))
		}
	}
	return nil
}

// CheckAllValidated verifies that all expected commands were validated. If any commands were expected but not validated,
// will invoke Fatalf on the *testing.T supplied to ValidatingExecer (tests can `defer v.CheckAllValidated()` to use this).
// Not part of any os.exec interface.
func (v *ValidatingExecer) CheckAllValidated() {
	if v.vCmd.commandIdx != len(v.vCmd.expectedCmdsRe)-1 {
		v.vCmd.t.Fatalf("Number of expected commands: %d did not match validated command count: %d",
			len(v.vCmd.expectedCmdsRe), v.vCmd.commandIdx+1)
	}
}

type (
	// RetryTestExecer is an OsExec implementation that does nop for for all Run/Start calls.
	// returning error for numCallsToFail, nil on all other calls
	RetryTestExecer struct {
		RCmd *retryTestCmd
	}
	// retryTestCmd the cmd structure for testing retries
	retryTestCmd struct {
		Cmd
		numCallsToFail int
		CallCount      int
	}
)

// NewRetryTestExecer creates an execer for testing command retries.  It is given the number of times
// the command should fail before returning success
func NewRetryTestExecer(numCallsToFail int) *RetryTestExecer {
	return &RetryTestExecer{RCmd: &retryTestCmd{numCallsToFail: numCallsToFail}}
}

// Command return an os exec Cmd object
func (rte *RetryTestExecer) Command(cmd string, args ...string) Cmd {
	rte.RCmd.Cmd = NewOsExec().Command(cmd, args...)
	return rte.RCmd
}

// Run override os exec's Run to return n failures (for n, set in NewRetryTestExecer)
// then nil error (success)
func (rtc *retryTestCmd) Run() error {
	rtc.CallCount++
	if rtc.CallCount <= rtc.numCallsToFail {
		return fmt.Errorf("failing call %d", rtc.CallCount)
	}
	return nil
}

// Start override os exec's Start to return n failures (for n, set in NewRetryTestExecer)
// then nil error (success)
func (rtc *retryTestCmd) Start() error {
	return rtc.Run()
}

// Wait override os exec's Wait returning nil
func (rtc *retryTestCmd) Wait() error {
	return nil
}

type (
	// SleepingExecer execer whose Run() function sleeps for a specified number of seconds
	SleepingExecer struct {
		sCmd   Cmd
		durSec int
	}
)

// NewSleepingExecer instantiates a SleepingExecer
func NewSleepingExecer(durSec int) *SleepingExecer {
	return &SleepingExecer{durSec: durSec}
}

// Command ignores the cmd and args passed in and creates an os exec Command object
func (se *SleepingExecer) Command(cmd string, args ...string) Cmd {
	// Create a real Command mainly for interface compatibility
	se.sCmd = NewOsExec().Command("sleep", fmt.Sprintf("%d", se.durSec))

	return se.sCmd
}

type (
	// NopExecer is an OsExec implementation that does nop for for all Run/Start calls.
	// returning nil
	NopExecer struct {
		NopCmd *nopCmd
	}
	// nopCmd the cmd structure for testing retries
	nopCmd struct {
		Cmd
	}
)

// NewNopExecer creates an execer for nop commands.
func NewNopExecer() *NopExecer {
	return &NopExecer{NopCmd: &nopCmd{}}
}

// Command return an os exec Cmd object that returns a nop command (the command sleeps for 0 seconds)
func (nope *NopExecer) Command(cmd string, args ...string) Cmd {
	nope.NopCmd.Cmd = NewOsExec().Command("sleep", "0") // use a nop command so it always returns no error
	return nope.NopCmd
}
