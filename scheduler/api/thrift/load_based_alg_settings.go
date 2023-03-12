package thrift

import (
	"time"

	"github.com/wisechengyi/scoot/scheduler/server"
)

// GetClassLoadPercents get the target load pcts for the classes
func GetClassLoadPercents(scheduler server.Scheduler) (map[string]int32, error) {
	return scheduler.GetClassLoadPercents()
}

// SetClassLoadPercents set the target worker load % for each job class
func SetClassLoadPercents(scheduler server.Scheduler, classLoads map[string]int32) error {
	return scheduler.SetClassLoadPercents(classLoads)
}

// GetRequestorToClassMap get map of requestor (reg exp) to class load pct
func GetRequestorToClassMap(scheduler server.Scheduler) (map[string]string, error) {
	return scheduler.GetRequestorToClassMap()
}

// SetRequestorToClassMap set the map of requestor (requestor value is reg exp) to class name
func SetRequestorToClassMap(scheduler server.Scheduler, requestorToClassMap map[string]string) error {
	return scheduler.SetRequestorToClassMap(requestorToClassMap)
}

// GetRebalanceMinimumDuration get the duration (min) that the rebalance threshold must be exceeded before
// triggering rebalance.  <= 0 implies no rebalancing
func GetRebalanceMinimumDuration(scheduler server.Scheduler) (time.Duration, error) {
	return scheduler.GetRebalanceMinimumDuration()
}

// SetRebalanceMinimumDuration get the duration (min) that the rebalance threshold must be exceeded before
// triggering rebalance.  <= 0 implies no rebalancing
func SetRebalanceMinimumDuration(scheduler server.Scheduler, duration time.Duration) error {
	return scheduler.SetRebalanceMinimumDuration(duration)
}

// GetRebalanceThreshold get the rebalance threshold.  The %s spread must exceed this for RebalanceMinimumDuration
// to trigger rebalance.  <= 0 implies no rebalancing
func GetRebalanceThreshold(scheduler server.Scheduler) (int32, error) {
	return scheduler.GetRebalanceThreshold()
}

// SetRebalanceThreshold get the rebalance threshold.  The %s spread must exceed this for RebalanceMinimumDuration
// to trigger rebalance.  <= 0 implies no rebalancing
func SetRebalanceThreshold(scheduler server.Scheduler, threshold int32) error {
	return scheduler.SetRebalanceThreshold(threshold)
}
