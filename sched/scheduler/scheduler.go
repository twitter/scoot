// Package scheduler provides the main job scheduling interface for Scoot
package scheduler

//go:generate mockgen -source=scheduler.go -package=scheduler -destination=scheduler_mock.go

import (
	"github.com/twitter/scoot/sched"
)

type Scheduler interface {
	ScheduleJob(jobDef sched.JobDefinition) (string, error)

	KillJob(jobId string) error
}
