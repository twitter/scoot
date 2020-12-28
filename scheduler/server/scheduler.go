// Package server provides the main job scheduling interface for Scoot
package server

//go:generate mockgen -source=scheduler.go -package=server -destination=scheduler_mock.go

import (
	"time"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/scheduler/domain"
)

type Scheduler interface {
	ScheduleJob(jobDef domain.JobDefinition) (string, error)

	KillJob(jobId string) error

	GetSagaCoord() saga.SagaCoordinator

	OfflineWorker(req domain.OfflineWorkerReq) error

	ReinstateWorker(req domain.ReinstateWorkerReq) error

	SetSchedulerStatus(maxTasks int) error

	GetSchedulerStatus() (int, int)

	GetClassLoadPercents() (map[string]int32, error)

	SetClassLoadPercents(classLoads map[string]int32) error

	GetRequestorToClassMap() (map[string]string, error)

	SetRequestorToClassMap(requestorToClassMap map[string]string) error

	GetRebalanceMinimumDuration() (time.Duration, error)

	SetRebalanceMinimumDuration(durationMin time.Duration) error

	GetRebalanceThreshold() (int32, error)

	SetRebalanceThreshold(durationMin int32) error
}

// SchedulingAlgorithm interface for the scheduling algorithm.  Implementations will compute the list of
// tasks to start and stop
type SchedulingAlgorithm interface {
	GetTasksToBeAssigned(jobs []*jobState, stat stats.StatsReceiver, cs *clusterState,
		requestors map[string][]*jobState) (startTasks []*taskState, stopTasks []*taskState)
}
