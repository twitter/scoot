package api

import (
	"github.com/twitter/scoot/scheduler/sched/scheduler"
)

/**
throttle the scheduler - set the max number of tasks it will allow
*/
func SetSchedulerStatus(scheduler scheduler.Scheduler, maxTasks int32) error {

	return scheduler.SetSchedulerStatus(int(maxTasks))
}
