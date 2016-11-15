// Package proc includes definitions for Scoot's
// Job, Task and associated structures.
// TODO this seems deprecated - Is this real?? DO I EXIST??
package proc

import (
	"github.com/scootdev/scoot/runner"
)

type JobType int

const (
	Unknown   JobType = 1
	IronTests JobType = 2
)

// A Task is part of a Job and defines the input Snapshot
// and command to be run for creation of a new snapshot.
type Task struct {
	ID       string
	Cmd      runner.Command
	Snapshot string
}

type JobDefinition struct {
	Tasks []Task
	Type  JobType
}

// A Job is the high-level concept of work for Scoot, composed
// of an ID string and slice of Tasks.
type Job struct {
	ID    string
	Tasks []Task
}

type Proc interface {
	RunJob(job JobDefinition) (Job, error)
}
