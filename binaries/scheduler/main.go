package main

import (
	"fmt"
	clusterimpl "github.com/scootdev/scoot/cloud/cluster/memory"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/distributor"
	queueimpl "github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/sched/worker/fake"
	"log"
	"sync"
)

func main() {
	fmt.Println("Starting Scheduler")

	// Create Cluster
	// TODO: replace with actual cluster implementation, currently dummy in memory cluster
	cluster := clusterimpl.NewCluster(clusterimpl.NewIdNodes(10), nil)
	dist, err := distributor.NewPoolDistributorFromCluster(cluster)
	if err != nil {
		log.Fatalf("Error subscribing to cluster: %v", err)
	}

	// Create Saga Log
	// TODO: Replace with Durable SagaLog, currently In Memory Only
	sagaCoordinator := saga.MakeInMemorySagaCoordinator()
	sched := scheduler.NewScheduler(dist, sagaCoordinator, fake.MakeWaitingNoopWorker)
	// TODO: Replace with Durable WorkQueue, currently in Memory Only
	workQueue := queueimpl.NewSimpleQueue(1000)

	var wg sync.WaitGroup
	wg.Add(1)

	// Go Routine which takes data from work queue and schedules it
	go func() {
		defer wg.Done()
		scheduler.GenerateWork(sched, workQueue.Chan())
	}()

	wg.Wait()
}
