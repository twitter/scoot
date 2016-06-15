package proc

import (
	"github.com/scootdev/scoot/runner"
)

type JobType int

const (
	Unknown   JobType = 1
	IronTests JobType = 2
)

type Task struct {
	ID       string
	Cmd      runner.Command
	Snapshot string
}

type JobDefinition struct {
	Tasks []Task
	Type  JobType
}

type Job struct {
	ID    string
	Tasks []Task
}

type Proc interface {
	RunJob(job JobDefinition) (Job, error)
}
