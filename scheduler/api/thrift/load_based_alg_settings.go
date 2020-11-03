package thrift

import (
	"github.com/twitter/scoot/scheduler/server"
)

/*
GetClassLoadPcts get the target load pcts for the classes
*/
func GetClassLoadPcts(scheduler server.Scheduler) map[string]int32 {

	return scheduler.GetClassLoadPcts()
}

/*
SetClassLoadPcts set the target worker load % for each job class
*/
func SetClassLoadPcts(scheduler server.Scheduler, classLoads map[string]int32) {

	scheduler.SetClassLoadPcts(classLoads)
}

/*
GetRequestorToClassMap get map of requestor (reg exp) to class load pct
*/
func GetRequestorToClassMap(scheduler server.Scheduler) map[string]string {

	return scheduler.GetRequestorToClassMap()
}

/*
SetRequestorToClassMap set the map of requestor (requestor value is reg exp) to class name
*/
func SetRequestorToClassMap(scheduler server.Scheduler, requestorToClassMap map[string]string) {

	scheduler.SetRequestorToClassMap(requestorToClassMap)
}
