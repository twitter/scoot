package api

import (
	"github.com/twitter/scoot/scheduler/sched/scheduler"
	"github.com/twitter/scoot/scheduler/scootapi/gen-go/scoot"
)

/**
Get the scheduler status: how many tasks is it currently handling and
it's current throttle value (-1 implies it is not throttled)
*/
func NewSchedulerStatus(numTasks int, maxTasks int) *scoot.SchedulerStatus {
	return &scoot.SchedulerStatus{
		CurrentTasks: int32(numTasks),
		MaxTasks:     int32(maxTasks),
	}
}

func GetSchedulerStatus(scheduler scheduler.Scheduler) (*scoot.SchedulerStatus, error) {

	var numTasks int
	var maxTasks int

	numTasks, maxTasks = scheduler.GetSchedulerStatus()
	return NewSchedulerStatus(numTasks, maxTasks), nil
}
