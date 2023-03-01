package thrift

import (
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/twitter/scoot/scheduler/server"
)

/*
*
Get the scheduler status: how many tasks is it currently handling and
it's current throttle value (-1 implies it is not throttled)
*/
func NewSchedulerStatus(numTasks int, maxTasks int) *scoot.SchedulerStatus {
	return &scoot.SchedulerStatus{
		CurrentTasks: int32(numTasks),
		MaxTasks:     int32(maxTasks),
	}
}

func GetSchedulerStatus(scheduler server.Scheduler) (*scoot.SchedulerStatus, error) {
	var numTasks int
	var maxTasks int

	numTasks, maxTasks = scheduler.GetSchedulerStatus()
	return NewSchedulerStatus(numTasks, maxTasks), nil
}
