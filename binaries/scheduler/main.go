package main

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/saga"
	ci "github.com/scootdev/scoot/sched/clusterimplementations"
	"github.com/scootdev/scoot/sched/queue/memory"
	sched "github.com/scootdev/scoot/sched/scheduler"
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
	cluster, clusterState := ci.DynamicLocalNodeClusterFactory(10)

	// Create Saga Log
	// TODO: Replace with Durable SagaLog, currently In Memory Only
	sagaCoordinator := saga.MakeInMemorySagaCoordinator()
	scheduler := sched.NewScheduler(cluster, clusterState, sagaCoordinator)

	// TODO: Replace with Durable WorkQueue, currently in Memory Only
	workQueue := memory.NewSimpleQueue(1000)

	handler := server.NewHandler(workQueue, sagaCoordinator)

	// Start API Server
	err := server.Serve(handler, addr, transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error serving Scoot API: ", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Go Routine which takes data from work queue and schedules it
	go func() {
		defer wg.Done()
		sched.GenerateWork(scheduler, workQueue.Chan())
	}()

	wg.Wait()
}
