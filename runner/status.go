package runner

import "fmt"

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
	// Request rejected for reasons unrelated to the user.
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

type LogTags struct {
	JobID  string
	TaskID string
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

	LogTags
}

func (p RunStatus) String() string {
	s := fmt.Sprintf("RunStatus -- RunID:%s # SnapshotID:%s # State:%s # JobID:%s # TaskID:%s",
		p.RunID, p.SnapshotID, p.State, p.JobID, p.TaskID)

	if p.State == COMPLETE {
		s += fmt.Sprintf(" # ExitCode:%d", p.ExitCode)
	}
	if p.State == FAILED || p.State == BADREQUEST {
		s += fmt.Sprintf(" # Error:%s", p.Error)
	}
	s += fmt.Sprintf(" # Stdout:%s # Stderr:%s", p.StdoutRef, p.StderrRef)

	return s
}

// Helper functions to create RunStatus

func AbortStatus(runID RunID, IDs LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = ABORTED
	r.LogTags = IDs
	return r
}

func TimeoutStatus(runID RunID, IDs LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = TIMEDOUT
	r.LogTags = IDs
	return r
}

func ErrorStatus(runID RunID, err error, IDs LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = FAILED
	r.Error = err.Error()
	r.LogTags = IDs
	return r
}

func BadRequestStatus(runID RunID, err error, IDs LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = BADREQUEST
	r.Error = err.Error()
	r.LogTags = IDs
	return r
}

func PendingStatus(runID RunID, IDs LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = PENDING
	r.LogTags = IDs
	return r
}

func RunningStatus(runID RunID, stdoutRef, stderrRef string, IDs LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = RUNNING
	r.StdoutRef = stdoutRef
	r.StderrRef = stderrRef
	r.LogTags = IDs
	return r
}

func CompleteStatus(runID RunID, snapshotID string, exitCode int, IDs LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = COMPLETE
	r.SnapshotID = snapshotID
	r.ExitCode = exitCode
	r.LogTags = IDs
	return r
}

func PreparingStatus(runID RunID, IDs LogTags) (r RunStatus) {
	r.RunID = runID
	r.State = PREPARING
	r.LogTags = IDs
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
