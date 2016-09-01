package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"

	// For putting into ice.MagicBag
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi/server"

	// For putting into jsonconfig.Options
	"github.com/scootdev/scoot/config/scootconfig"
)

var addr = flag.String("addr", "localhost:9090", "Bind address for api server.")
var httpPort = flag.Int("http_port", 9091, "port to serve http on")
var cfgText = flag.String("sched_config", "", "Scheduler Configuration.")

type servers struct {
	thrift thrift.TServer
	http   *endpoints.TwitterServer
	sched  scheduler.Scheduler
	queue  queue.Queue
	stat   stats.StatsReceiver
}

func makeServers(thrift thrift.TServer, http *endpoints.TwitterServer, sched scheduler.Scheduler, queue queue.Queue, stat stats.StatsReceiver) servers {
	return servers{thrift, http, sched, queue, stat}
}

func main() {
	log.Println("Starting Cloud Scoot API Server & Scheduler")
	flag.Parse()

	bag := ice.NewMagicBag()
	bag.PutMany(
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(*addr) },
		endpoints.MakeStatsReceiver,
		server.MakeServer,
		server.NewHandler,
		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(fmt.Sprintf(":%d", *httpPort), s)
		},
		makeServers,
		saga.MakeSagaCoordinator,
		saga.MakeInMemorySagaLog,
		scheduler.NewStatefulSchedulerFromCluster,
		thrift.NewTTransportFactory,
		func() thrift.TProtocolFactory {
			return thrift.NewTBinaryProtocolFactoryDefault()
		},
	)

	schema := jsonconfig.Schema(map[string]jsonconfig.Implementations{
		"Cluster": {
			"memory": &scootconfig.ClusterMemoryConfig{},
			"local":  &scootconfig.ClusterLocalConfig{},
			"": &scootconfig.ClusterMemoryConfig{
				Type:  "memory",
				Count: 10,
			},
		},
		"Queue": {
			"memory": &scootconfig.QueueMemoryConfig{},
			"": &scootconfig.QueueMemoryConfig{
				Type:     "memory",
				Capacity: 1000,
			},
		},
		"Workers": {
			"local": &scootconfig.WorkersLocalConfig{},
			"rpc":   &scootconfig.WorkersThriftConfig{},
			"":      &scootconfig.WorkersLocalConfig{Type: "local"},
		},
	})

	mod, err := schema.Parse([]byte(*cfgText))
	if err != nil {
		log.Fatal("Error configuring Scoot API: ", err)
	}
	bag.InstallModule(mod)

	// TODO(dbentley): we may want to refactor all of the code below here to be
	// logic in a library instead of copied into each main, but for now be explicit.
	var servers servers
	err = bag.Extract(&servers)
	if err != nil {
		log.Fatal("Error injecting servers", err)
	}

	go func() {
		scheduler.GenerateWork(servers.sched, servers.queue.Chan(), servers.stat)
	}()

	// TODO(dbentley): if one fails and the other doesn't, we should do
	// something smarter...

	errCh := make(chan error)
	go func() {
		errCh <- servers.http.Serve()
	}()
	go func() {
		errCh <- servers.thrift.Serve()

	}()
	log.Fatal("Error serving: ", <-errCh)
}

// import (
// 	"flag"
// 	"fmt"
// 	"github.com/apache/thrift/lib/go/thrift"
// 	"github.com/scootdev/scoot/cloud/cluster"
// 	"github.com/scootdev/scoot/common/stats"
// 	"github.com/scootdev/scoot/saga"
// 	queueImpl "github.com/scootdev/scoot/sched/queue/memory"
// 	"github.com/scootdev/scoot/sched/scheduler"
// 	worker "github.com/scootdev/scoot/sched/worker/fake"
// 	"github.com/scootdev/scoot/scootapi/server"
// 	"log"
// 	"sync"
// )

// var addr = flag.String("addr", "localhost:9090", "Bind address for api server.")

// func main() {

// 	log.Println("Starting Cloud Scoot API Server & Scheduler")
// 	addr := "localhost:9090"

// 	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
// 	transportFactory := thrift.NewTTransportFactory()

// 	// Create Cluster
// 	// TODO: replace with actual cluster implementation, currently dummy in memory cluster
// 	workerNodes := []cluster.Node{}
// 	for i := 0; i < 5; i++ {
// 		workerNodes = append(workerNodes, cluster.NewIdNode(fmt.Sprintf("inmemory%d", i)))
// 	}
// 	cl := cluster.NewCluster(workerNodes, nil)

// 	// Create Saga Log
// 	// TODO: Replace with Durable SagaLog, currently In Memory Only
// 	sagaCoordinator := saga.MakeInMemorySagaCoordinator()
// 	wf := worker.MakeNoopWorker
// 	stats := stats.NilStatsReceiver()

// 	clSub := cl.Subscribe()
// 	sched := scheduler.NewStatefulScheduler(clSub.InitialMembers, clSub.Updates, sagaCoordinator, wf, stats, false)
// 	// TODO: Replace with Durable WorkQueue, currently in Memory Only
// 	workQueue := queueImpl.NewSimpleQueue(1000)
// 	handler := server.NewHandler(workQueue, sagaCoordinator, stats)

// 	var wg sync.WaitGroup
// 	wg.Add(2)

// 	apiServer := server.MakeServer(handler, thrift.NewTServerSocket(*addr), transportFactory, protocolFactory)

// 	// Start API Server
// 	go func() {
// 		log.Println("Starting API Server")
// 		defer wg.Done()
// 		apiServer.http.Serve()z
// 		if err != nil {
// 			log.Fatal("Error serving Scoot API: ", err)
// 		}
// 	}()

// 	// Go Routine which takes data from work queue and schedules it
// 	go func() {
// 		log.Println("Starting Scheduler")
// 		defer wg.Done()
// 		scheduler.GenerateWork(sched, workQueue.Chan(), stats)
// 	}()

// 	wg.Wait()
// }*/
