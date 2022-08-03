package runner

import (
	"fmt"

	"github.com/twitter/scoot/common/errors"
	"github.com/twitter/scoot/common/log/tags"
)

type RunID string
type RunState int

const (
	// Catch-all for indeterminate RunStates. Also an "end state"
	UNKNOWN RunState = iota

	// Waiting to run
	PENDING

	// Running
	RUNNING

	// States below are "end states"
	// a Run in an "end state" will not change its state

	// Succeeded or failed yielding an exit code
	COMPLETE

	// Run mechanism failed and is no longer running
	FAILED

	// User requested that the run be killed, or task preempted by Scheduler
	ABORTED

	// Run timed out and was killed
	TIMEDOUT
)

func (p RunState) IsDone() bool {
	return p == COMPLETE || p == FAILED || p == ABORTED || p == TIMEDOUT || p == UNKNOWN
}

func (p RunState) String() string {
	switch p {
	case UNKNOWN:
		return "UNKNOWN"
	case PENDING:
		return "PENDING"
	case RUNNING:
		return "RUNNING"
	case COMPLETE:
		return "COMPLETE"
	case FAILED:
		return "FAILED"
	case ABORTED:
		return "ABORTED"
	case TIMEDOUT:
		return "TIMEDOUT"
	default:
		panic(fmt.Sprintf("Unexpected RunState %v", int(p)))
	}
}

// Returned by the coordinator when a run request is made.
type RunStatus struct {
	RunID RunID
	State RunState
	tags.LogTags

	// Fields below are optional and only exist for certain states

	// References to stdout and stderr, not their text
	// Runner impls might not provide valid refs for all States (e.g. failure before creation of refs)
	StdoutRef string
	StderrRef string
	// Only valid if State == COMPLETE
	SnapshotID string
	// Only valid if State == (COMPLETE || FAILED)
	ExitCode errors.ExitCode
	// Only valid if State == (COMPLETE || FAILED || ABORTED)
	Error string
}

func (p RunStatus) String() string {
	s := fmt.Sprintf(`RunStatus -- RunID: %s # State: %s # JobID: %s # TaskID: %s # Tag: %s`,
		p.RunID, p.State, p.JobID, p.TaskID, p.Tag)
	if p.StdoutRef != "" {
		s += fmt.Sprintf(" # Stdout: %s", p.StdoutRef)
	}
	if p.StderrRef != "" {
		s += fmt.Sprintf(" # Stderr: %s", p.StderrRef)
	}
	if p.State == COMPLETE {
		s += fmt.Sprintf(" # SnapshotID: %s", p.SnapshotID)
	}
	if p.State == COMPLETE || p.State == FAILED {
		s += fmt.Sprintf(" # ExitCode: %d", p.ExitCode)
	}
	if p.State == COMPLETE || p.State == FAILED || p.State == ABORTED {
		s += fmt.Sprintf(" # Error: %s", p.Error)
	}
	return s
}

// Helper functions to create RunStatus

func AbortStatus(runID RunID, tags tags.LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = ABORTED
	r.LogTags = tags
	return r
}

func TimeoutStatus(runID RunID, tags tags.LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = TIMEDOUT
	r.LogTags = tags
	return r
}

func FailedStatus(runID RunID, err *errors.ExitCodeError, tags tags.LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = FAILED
	r.Error = err.Error()
	r.ExitCode = err.GetExitCode()
	r.LogTags = tags
	return r
}

func PendingStatus(runID RunID, tags tags.LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = PENDING
	r.LogTags = tags
	return r
}

func RunningStatus(runID RunID, stdoutRef, stderrRef string, tags tags.LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = RUNNING
	r.StdoutRef = stdoutRef
	r.StderrRef = stderrRef
	r.LogTags = tags
	return r
}

func CompleteStatus(runID RunID, snapshotID string, exitCode errors.ExitCode, tags tags.LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = COMPLETE
	r.SnapshotID = snapshotID
	r.ExitCode = exitCode
	r.LogTags = tags
	return r
}

// This is for overall runner status, just 'initialized' status and error for now.
type ServiceStatus struct {
	Initialized bool
	IsHealthy   bool
	Error       error
}

func (s ServiceStatus) String() string {
	return fmt.Sprintf("--- Service Status ---\n\tInitialized : %t\n\tIsHealthy : %t\n",
		s.Initialized, s.IsHealthy)
}
