package main

import (
	"github.com/apache/thrift/lib/go/thrift"
	clusterimpl "github.com/scootdev/scoot/cloud/cluster/memory"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/distributor"
	queueimpl "github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/sched/worker/fake"
	"github.com/scootdev/scoot/scootapi/server"
	"log"
	"sync"
)

func main() {
	log.Println("Starting Cloud Scoot API Server & Scheduler")
	addr := "localhost:9090"

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

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

	handler := server.NewHandler(workQueue, sagaCoordinator)

	var wg sync.WaitGroup
	wg.Add(2)

	// Start API Server
	go func() {
		log.Println("Starting API Server")
		defer wg.Done()
		err := server.Serve(handler, addr, transportFactory, protocolFactory)
		if err != nil {
			log.Fatal("Error serving Scoot API: ", err)
		}
	}()

	// Go Routine which takes data from work queue and schedules it
	go func() {
		log.Println("Starting Scheduler")
		defer wg.Done()
		scheduler.GenerateWork(sched, workQueue.Chan())
	}()

	wg.Wait()
}
