package sched

/*
 * Message representing a Job, Scoot can Schedule
 */
type Job struct {
	Id      string
	JobType string
	Tasks   []Task
}

/*
 * Message representing a Task that is part of a Scoot Job.
 */
type Task struct {
	Id         string
	Command    []string // Argv to run
	SnapshotId string
}

type JobState int

const (
	// An unambiguous 0-value.
	UNKNOWN JobState = iota
	// Waiting to run.
	PENDING
	// Running
	RUNNING
	// Done
	DONE
)

type JobStatus struct {
	ID    string
	State JobState
}

func PendingStatus(jobID string) (s JobStatus) {
	s.ID = jobID
	s.State = PENDING
	return s
}

func UnknownStatus(jobID string) (s JobStatus) {
	s.ID = jobID
	s.State = UNKNOWN
	return s
}
