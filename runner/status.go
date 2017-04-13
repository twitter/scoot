package runner

import (
	"bytes"
	"fmt"
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
	// Run mechanism failed and run is no longer active. Retry may or may not work.
	FAILED
	// User requested that the run be killed.
	ABORTED
	// Operation timed out and was killed.
	TIMEDOUT
	// Invalid or error'd request. Original runner state not affected. Retry may work after mutation.
	BADREQUEST
)

func (p RunState) IsDone() bool {
	return p == COMPLETE || p == FAILED || p == ABORTED || p == TIMEDOUT
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
}

func (p RunStatus) String() string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "--- Run Status ---\n\tRun:\t\t%s\n\tSnapshot:\t\t%s\n\tState:\t\t%s\n", p.RunID, p.SnapshotID, p.State)

	if p.State == COMPLETE {
		fmt.Fprintf(&b, "\tExitCode:\t%d\n", p.ExitCode)
	}
	if p.State == FAILED || p.State == BADREQUEST {
		fmt.Fprintf(&b, "\tError:\t\t%s\n", p.Error)
	}

	fmt.Fprintf(&b, "\tStdout:\t\t%s\n\tStderr:\t\t%s\n", p.StdoutRef, p.StderrRef)

	return b.String()
}

// Helper functions to create RunStatus

func AbortStatus(runID RunID) (r RunStatus) {
	r.RunID = runID
	r.State = ABORTED
	return r
}

func TimeoutStatus(runID RunID) (r RunStatus) {
	r.RunID = runID
	r.State = TIMEDOUT
	return r
}

func ErrorStatus(runID RunID, err error) (r RunStatus) {
	r.RunID = runID
	r.State = FAILED
	r.Error = err.Error()
	return r
}

func BadRequestStatus(runID RunID, err error) (r RunStatus) {
	r.RunID = runID
	r.State = BADREQUEST
	r.Error = err.Error()
	return r
}

func PendingStatus(runID RunID) (r RunStatus) {
	r.RunID = runID
	r.State = PENDING
	return r
}

func RunningStatus(runID RunID, stdoutRef, stderrRef string) (r RunStatus) {
	r.RunID = runID
	r.State = RUNNING
	r.StdoutRef = stdoutRef
	r.StderrRef = stderrRef
	return r
}

func CompleteStatus(runID RunID, snapshotID string, exitCode int) (r RunStatus) {
	r.RunID = runID
	r.State = COMPLETE
	r.SnapshotID = snapshotID
	r.ExitCode = exitCode
	return r
}

func PreparingStatus(runID RunID) (r RunStatus) {
	r.RunID = runID
	r.State = PREPARING
	return r
}

// This is for overall runner status, just 'initialized' status for now.
type ServiceStatus struct {
	Initialized bool
}

func (s ServiceStatus) String() string {
	return fmt.Sprintf("--- Service Status ---\n\tInitialized:%t\n", s.Initialized)
}
