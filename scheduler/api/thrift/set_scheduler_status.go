package thrift

import (
	"github.com/twitter/scoot/scheduler/server"
)

/**
throttle the scheduler - set the max number of tasks it will allow
*/
func SetSchedulerStatus(scheduler server.Scheduler, maxTasks int32) error {
	return scheduler.SetSchedulerStatus(int(maxTasks))
}
