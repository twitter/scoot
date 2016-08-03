package scheduler

import (
	"sync"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/worker"
)

type Scheduler interface {
	Sagas() saga.SagaCoordinator
	Wait() error
}

func NewSchedulerFromCluster(cl cluster.Cluster, q chan queue.WorkItem, sagaLog saga.SagaLog, workerFactory worker.WorkerFactory) Scheduler {
	sub := cl.Subscribe()

	s := NewScheduler(sub.Updates, q, sagaLog, workerFactory).(*coordinator)

	// Populate initial nodes
	updates := []cluster.NodeUpdate{}
	for _, n := range sub.InitialMembers {
		updates = append(updates, cluster.NewAdd(n))
	}
	s.cluster <- updates
	return s
}

func NewScheduler(cl chan []cluster.NodeUpdate, q chan queue.WorkItem, sagaLog saga.SagaLog, workerFactory worker.WorkerFactory) Scheduler {
	sc := saga.MakeSagaCoordinator(sagaLog)

	s := &coordinator{
		cluster: cl,
		queue:   q,
		replyCh: make(chan reply),

		sc:            sc,
		workerFactory: workerFactory,

		workers:    make(map[cluster.NodeId]worker.Worker),
		queueItems: make(map[string]queue.WorkItem),
		sagas:      make(map[string]chan saga.Message),

		st: &schedulerState{},

		writers: &sync.WaitGroup{},

		errCh: make(chan error),
	}
	s.writers.Add(2) // cluster and queue

	go s.closeWhenDone()
	go s.loop()

	// TODO(dbentley): recover in-process jobs from saga log

	return s
}
