package api

import (
	"github.com/twitter/scoot/sched/scheduler"
)

func ReinstateWorker(id string, scheduler scheduler.Scheduler) error {
	return scheduler.ReinstateWorker(id)
}
