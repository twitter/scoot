// Package server provides the main job scheduling interface for Scoot
package server

//go:generate mockgen -source=scheduler.go -package=server -destination=scheduler_mock.go

import (
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

	GetClassLoadPcts() map[string]int32

	SetClassLoadPcts(classLoads map[string]int32)

	GetRequestorToClassMap() map[string]string

	SetRequestorToClassMap(requestorToClassMap map[string]string)
}

type SchedulingAlgorithm interface {
	GetTasksToBeAssigned(jobs []*jobState, stat stats.StatsReceiver, cs *clusterState,
		requestors map[string][]*jobState, cfg SchedulerConfig) []*taskState
}
