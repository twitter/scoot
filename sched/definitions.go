package sched

// Job is the job Scoot can schedule
type Job struct {
	Id  string
	Def JobDefinition
}

// JobDefinition is the definition the client sent us
type JobDefinition struct {
	JobType string
	Tasks   map[string]TaskDefinition
}

// Task is one task to run
type TaskDefinition struct {
	Command    Command
	SnapshotId string
}

// Command is the argv to run
type Command struct {
	Argv []string
	// We may extend this with more configuration,
	// E.g. environment variables
}

// Status for Job & Tasks
type Status int

const (
	NotStarted Status = iota
	InProgress
	Completed
	RollingBack
	RolledBack
)
