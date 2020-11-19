package thrift

import (
	"github.com/twitter/scoot/scheduler/server"
)

// GetClassLoadPcts get the target load pcts for the classes
func GetClassLoadPcts(scheduler server.Scheduler) (map[string]int32, error) {
	return scheduler.GetClassLoadPcts()
}

// SetClassLoadPcts set the target worker load % for each job class
func SetClassLoadPcts(scheduler server.Scheduler, classLoads map[string]int32) error {
	return scheduler.SetClassLoadPcts(classLoads)
}

// GetRequestorToClassMap get map of requestor (reg exp) to class load pct
func GetRequestorToClassMap(scheduler server.Scheduler) (map[string]string, error) {
	return scheduler.GetRequestorToClassMap()
}

// SetRequestorToClassMap set the map of requestor (requestor value is reg exp) to class name
func SetRequestorToClassMap(scheduler server.Scheduler, requestorToClassMap map[string]string) error {
	return scheduler.SetRequestorToClassMap(requestorToClassMap)
}

// GetRebalanceMinDuration get the duration (min) that the rebalance threshold must be exceeded before
// triggering rebalance.  <= 0 implies no rebalancing
func GetRebalanceMinDuration(scheduler server.Scheduler) (int32, error) {
	return scheduler.GetRebalanceMinDuration()
}

// SetRebalanceMinDuration get the duration (min) that the rebalance threshold must be exceeded before
// triggering rebalance.  <= 0 implies no rebalancing
func SetRebalanceMinDuration(scheduler server.Scheduler, duration int32) error {
	return scheduler.SetRebalanceMinDuration(duration)
}

// GetRebalanceThreshold get the rebalance threshold.  The %s spread must exceed this for RebalanceMinDuration
// to trigger rebalance.  <= 0 implies no rebalancing
func GetRebalanceThreshold(scheduler server.Scheduler) (int32, error) {
	return scheduler.GetRebalanceThreshold()
}

// SetRebalanceThreshold get the rebalance threshold.  The %s spread must exceed this for RebalanceMinDuration
// to trigger rebalance.  <= 0 implies no rebalancing
func SetRebalanceThreshold(scheduler server.Scheduler, duration int32) error {
	return scheduler.SetRebalanceThreshold(duration)
}
