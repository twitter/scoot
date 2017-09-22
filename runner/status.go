package runner

import (
	"fmt"

	"github.com/twitter/scoot/common/log/tags"
)

type RunID string
type RunState int

const (
	// An unambiguous 0-value.
	UNKNOWN RunState = iota
	// Waiting to run.
	PENDING
	// Preparing to run (e.g., checking out the Snapshot)
	PREPARING
	// Running
	RUNNING

	// States below are end states
	// a Run in an end state will not change its state

	// Succeeded or failed yielding an exit code. Only state with an exit code.
	COMPLETE
	// Run mechanism failed in an expected way and is no longer running.
	FAILED
	// User requested that the run be killed.
	ABORTED
	// Operation timed out and was killed.
	TIMEDOUT
	// Request rejected due to unexpected failure.
	BADREQUEST
)

func (p RunState) IsDone() bool {
	return p == COMPLETE || p == FAILED || p == ABORTED || p == TIMEDOUT || p == UNKNOWN || p == BADREQUEST
}

func (p RunState) String() string {
	switch p {
	case UNKNOWN:
		return "UNKNOWN"
	case PENDING:
		return "PENDING"
	case PREPARING:
		return "PREPARING"
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
	case BADREQUEST:
		return "BADREQUEST"
	default:
		panic(fmt.Sprintf("Unexpected RunState %v", int(p)))
	}
}

// Returned by the coordinator when a run request is made.
type RunStatus struct {
	RunID RunID

	State RunState
	// References to stdout and stderr, not their text
	// Runner impls shall provide valid refs for all States (but optionally may not for UNKNOWN/BADREQUEST).
	StdoutRef string
	StderrRef string

	// Only valid if State == COMPLETE
	SnapshotID string
	ExitCode   int

	// Only valid if State == (FAILED || BADREQUEST)
	Error string

	tags.LogTags
}

func (p RunStatus) String() string {
	s := fmt.Sprintf("RunStatus -- RunID: %s # SnapshotID: %s # State: %s # JobID: %s # TaskID: %s # Tag: %s",
		p.RunID, p.SnapshotID, p.State, p.JobID, p.TaskID, p.Tag)

	if p.State == COMPLETE {
		s += fmt.Sprintf(" # ExitCode: %d", p.ExitCode)
	}
	if p.State == FAILED || p.State == BADREQUEST {
		s += fmt.Sprintf(" # Error: %s", p.Error)
	}
	s += fmt.Sprintf(" # Stdout: %s # Stderr: %s", p.StdoutRef, p.StderrRef)

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

func BadRequestStatus(runID RunID, err error, tags tags.LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = BADREQUEST
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

func PreparingStatus(runID RunID, tags tags.LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = PREPARING
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
