package scheduler

import (
	"sync"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/workerapi"
)

type Scheduler interface {
	Run(def sched.JobDefinition) (string, error)
	GetState(jobId string) (*saga.SagaState, error)
	GetStatus(jobId string) (JobStatus, error)
	WaitForError() error
	WaitForShutdown()
}

func NewSchedulerFromCluster(cl cluster.Cluster, q queue.Queue, sagaLog saga.SagaLog, workerFactory worker.WorkerFactory) Scheduler {
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

func NewScheduler(cl chan []cluster.NodeUpdate, q queue.Queue, sagaLog saga.SagaLog, workerFactory worker.WorkerFactory) Scheduler {
	sc := saga.MakeSagaCoordinator(sagaLog)

	s := &coordinator{
		queue:         q,
		sc:            sc,
		workerFactory: workerFactory,

		cluster: cl,
		queueCh: q.Chan(),
		replyCh: make(chan reply),

		workers:      make(map[cluster.NodeId]workerapi.Worker),
		queueItems:   make(map[string]queue.WorkItem),
		sagas:        make(map[string]chan saga.Message),
		inflightRpcs: 0,

		st: &schedulerState{},

		err:    nil,
		errWg:  &sync.WaitGroup{},
		loopWg: &sync.WaitGroup{},

		actives: &jobStates{
			actives: make(map[string]*saga.SagaState),
		},

		l: &loggingListener{},
	}

	s.errWg.Add(1)
	s.loopWg.Add(1)

	go s.loop()

	// TODO(dbentley): recover in-process jobs from saga log

	return s
}

func (s *coordinator) Run(def sched.JobDefinition) (string, error) {
	return s.queue.Enqueue(def)
}
