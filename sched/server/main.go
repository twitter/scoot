package main

import (
	"fmt"
	"github.com/scootdev/scoot/saga"
	ci "github.com/scootdev/scoot/sched/clusterimplementations"
	sched "github.com/scootdev/scoot/sched/scheduler"
)

func main() {
	fmt.Println("Starting Scheduler")

	// Create Cluster
	// TODO: replace with actual cluster implementation, currently dummy in memory cluster
	cluster, clusterState := ci.DynamicLocalNodeClusterFactory(10)

	// Create Saga Log
	// TODO: Replace with Durable SagaLog, currently In Memory Only
	saga := saga.MakeInMemorySaga()

	scheduler := sched.NewScheduler(cluster, clusterState, saga)

	scheduler.Start()

	// TODO: pull work off of WorkQueue and schedule

}
