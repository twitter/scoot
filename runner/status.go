package runner

import (
	"fmt"

	"github.com/twitter/scoot/bazel/execution/bazelapi"
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
	case UNKNOWN:
		return "UNKNOWN"
	default:
		panic(fmt.Sprintf("Unexpected RunState %v", int(p)))
	}
}

// Returned by the coordinator when a run request is made.
type RunStatus struct {
	RunID RunID

	State RunState
	// References to stdout and stderr, not their text
	// Runner impls may not provide valid refs for all States (e.g. failure before creation of refs)
	StdoutRef string
	StderrRef string

	// Only valid if State == COMPLETE
	SnapshotID string
	// Only valid if State == (COMPLETE || FAILED)
	ExitCode int

	// Only valid if State == (COMPLETE || FAILED || ABORTED)
	Error string

	tags.LogTags
	ActionResult *bazelapi.ActionResult
}

func (p RunStatus) String() string {
	s := fmt.Sprintf(`RunStatus -- RunID: %s # SnapshotID: %s 
		# State: %s # JobID: %s # TaskID: %s # Tag: %s 
		# ExitCode: %s # Error: %s # Stdout: %s # Stderr: %s 
		# ActionResult: %s`,
		p.RunID, p.SnapshotID, p.State, p.JobID, p.TaskID,
		p.Tag, p.ExitCode, p.Error, p.StdoutRef, p.StderrRef,
		p.ActionResult)
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

func FailedStatus(runID RunID, err error, tags tags.LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = FAILED
	r.Error = err.Error()
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

func CompleteStatus(runID RunID, snapshotID string, exitCode int, tags tags.LogTags) (r RunStatus) {
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
	Error       error
}

func (s ServiceStatus) String() string {
	return fmt.Sprintf("--- Service Status ---\n\tInitialized:%t\n", s.Initialized)
}
