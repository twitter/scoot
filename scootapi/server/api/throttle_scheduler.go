package api

import (
	"github.com/twitter/scoot/sched/scheduler"
)

/**
throttle the scheduler - set the max number of tasks it will allow
*/
func ThrottleScheduler(scheduler scheduler.Scheduler, maxTasks int32) error {

	return scheduler.Throttle(int(maxTasks))
}
