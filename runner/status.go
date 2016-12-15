package runner

import (
	"bytes"
	"fmt"
)

type RunId string
type SnapshotId string
type ProcessState int

const (
	// An unambiguous 0-value.
	UNKNOWN ProcessState = iota
	// Waiting to run.
	PENDING
	// Preparing to run (e.g., checking out the Snapshot)
	PREPARING
	// Running
	RUNNING

	// States below are end states
	// a Process in an end state will not change its state

	// Succeeded or failed yielding an exit code. Only state with an exit code.
	COMPLETE
	// Run mechanism failed and run is no longer active. Retry may or may not work.
	FAILED
	// User requested that the run be killed.
	ABORTED
	// Operation timed out and was killed.
	TIMEDOUT
	// Invalid or error'd request. Original runner state not affected. Retry may work after mutation.
	BADREQUEST
)

func (p ProcessState) IsDone() bool {
	return p == COMPLETE || p == FAILED || p == ABORTED || p == TIMEDOUT
}

func (p ProcessState) String() string {
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
		panic(fmt.Sprintf("Unexpected ProcessState %v", int(p)))
	}
}

// Returned by the coordinator when a run request is made.
type ProcessStatus struct {
	RunId RunId

	State ProcessState
	// References to stdout and stderr, not their text
	// Runner impls shall provide valid refs for all States (but optionally may not for UNKNOWN/BADREQUEST).
	StdoutRef string
	StderrRef string

	// Only valid if State == COMPLETE
	SnapshotId SnapshotId
	ExitCode   int

	// Only valid if State == (FAILED || BADREQUEST)
	Error string
}

func (p ProcessStatus) String() string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "--- Process Status ---\n\tRun:\t\t%s\n\tSnapshot:\t\t%s\n\tState:\t\t%s\n", p.RunId, p.SnapshotId, p.State)

	if p.State == COMPLETE {
		fmt.Fprintf(&b, "\tExitCode:\t%d\n", p.ExitCode)
	}
	if p.State == FAILED || p.State == BADREQUEST {
		fmt.Fprintf(&b, "\tError:\t\t%s\n", p.Error)
	}

	fmt.Fprintf(&b, "\tStdout:\t\t%s\n\tStderr:\t\t%s\n", p.StdoutRef, p.StderrRef)

	return b.String()
}

// Helper functions to create ProcessStatus

func AbortStatus(runId RunId) (r ProcessStatus) {
	r.RunId = runId
	r.State = ABORTED
	return r
}

func TimeoutStatus(runId RunId) (r ProcessStatus) {
	r.RunId = runId
	r.State = TIMEDOUT
	return r
}

func ErrorStatus(runId RunId, err error) (r ProcessStatus) {
	r.RunId = runId
	r.State = FAILED
	r.Error = err.Error()
	return r
}

func BadRequestStatus(runId RunId, err error) (r ProcessStatus) {
	r.RunId = runId
	r.State = BADREQUEST
	r.Error = err.Error()
	return r
}

func PendingStatus(runId RunId) (r ProcessStatus) {
	r.RunId = runId
	r.State = PENDING
	return r
}

func RunningStatus(runId RunId, stdoutRef, stderrRef string) (r ProcessStatus) {
	r.RunId = runId
	r.State = RUNNING
	r.StdoutRef = stdoutRef
	r.StderrRef = stderrRef
	return r
}

func CompleteStatus(runId RunId, snapshotId SnapshotId, exitCode int) (r ProcessStatus) {
	r.RunId = runId
	r.State = COMPLETE
	r.SnapshotId = snapshotId
	r.ExitCode = exitCode
	return r
}

func PreparingStatus(runId RunId) (r ProcessStatus) {
	r.RunId = runId
	r.State = PREPARING
	return r
}
