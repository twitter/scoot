package api

import (
	"github.com/twitter/scoot/sched/scheduler"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
)

/**
Get the scheduler status: is it receiving jobs, how many tasks is it currently handling and
it's current throttle value (-1 implies it is not throttled)
*/
func NewSchedulerStatus(receiving bool, numTasks int, throttle int) *scoot.SchedulerStatus {
	return &scoot.SchedulerStatus{
		ReceivingJobs	: receiving,
		CurrentTasks 	: int32(numTasks),
		MaxTasks		: int32(throttle),
		}
}

func GetSchedulerStatus(scheduler scheduler.Scheduler) (*scoot.SchedulerStatus, error) {

	var receiving bool
	var numTasks int
	var throttle int

	receiving, numTasks, throttle = scheduler.GetSchedulerStatus()
	return NewSchedulerStatus(receiving, numTasks, throttle), nil
}

