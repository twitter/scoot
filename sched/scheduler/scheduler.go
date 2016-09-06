package scheduler

//go:generate mockgen -source=scheduler.go -package=scheduler -destination=scheduler_mock.go

import (
	"github.com/scootdev/scoot/sched"
)

type Scheduler interface {
	ScheduleJob(job sched.Job) error
}
