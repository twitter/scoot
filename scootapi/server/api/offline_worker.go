package api

import (
	"github.com/twitter/scoot/sched/scheduler"
)

func OfflineWorker(id string, scheduler scheduler.Scheduler) error {
	return scheduler.OfflineWorker(id)
}
