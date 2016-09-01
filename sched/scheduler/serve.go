package scheduler

import (
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/saga"
	dist "github.com/scootdev/scoot/sched/distributor"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/worker"
)

func MakeAndStartScheduler(
	nodes *dist.PoolDistributor,
	sc saga.SagaCoordinator,
	workerFactory worker.WorkerFactory,
	stat stats.StatsReceiver,
	q queue.Queue) Scheduler {
	result := NewScheduler(nodes, sc, workerFactory, stat)
	go func() {
		GenerateWork(result, q.Chan(), stat)
	}()
	return result
}
