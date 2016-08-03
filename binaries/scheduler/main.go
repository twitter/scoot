package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/cloud/cluster"
	clusterimpl "github.com/scootdev/scoot/cloud/cluster/memory"
	fakeexecer "github.com/scootdev/scoot/runner/execer/fake"
	localrunner "github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/saga"
	queueimpl "github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/scootapi/server"
	"github.com/scootdev/scoot/workerapi"
	"github.com/scootdev/scoot/workerapi/client"
	runnerworker "github.com/scootdev/scoot/workerapi/runner"
)

//TODO: we'll want more flexibility with startup configuration, maybe something like:
//  {"cluster": {"type": "static"},
//   "initial_nodes": ["localhost:2345", "localhost:2346"],
//   "workerFactory": "thrift"}
//
var addr = flag.String("addr", "localhost:9090", "Bind address for api server.")
var workers = flag.String("workers", "", "Comma separated list of workers (host:port,...)|NUM:mem.")

func main() {
	flag.Parse()

	cluster, workerFactory := makeCluster()

	// TODO: Replace with Durable SagaLog, currently In Memory Only
	sagaLog := saga.MakeInMemorySagaLog()

	// TODO: Replace with Durable WorkQueue, currently in Memory Only
	workQueue := queueimpl.NewSimpleQueue(1000)

	s := scheduler.NewSchedulerFromCluster(cluster, workQueue, sagaLog, workerFactory)

	log.Println("Starting API Server")
	err := server.Serve(s, *addr, thrift.NewTTransportFactory(), thrift.NewTBinaryProtocolFactoryDefault())
	if err != nil {
		log.Fatal("Error serving Scoot API: ", err)
	}
}

func makeCluster() (cluster.Cluster, worker.WorkerFactory) {
	workersList := strings.Split(*workers, ",")
	workerNodes := []cluster.Node{}
	if len(workersList) == 1 && (workersList[0] == "" || strings.Contains(workersList[0], ":mem")) {
		//Keep the original behavior for now if no workers specified on cmdline.
		numNodes := 10
		if workersList[0] != "" {
			numNodes, _ = strconv.Atoi(strings.Split(workersList[0], ":")[0])
		}
		workersList = []string{}
		for idx := 0; idx < numNodes; idx++ {
			workersList = append(workersList, fmt.Sprintf("inmemory%d", idx))
		}
	}
	for _, worker := range workersList {
		//TODO: methods to set/get an actual addr from cluster.Node, using Node.Id for now.
		workerNodes = append(workerNodes, clusterimpl.NewIdNode(worker))
	}

	// TODO: replace with actual cluster implementation, currently dummy in memory cluster
	cl := clusterimpl.NewCluster(workerNodes, nil)

	if strings.Contains(string(workerNodes[0].Id()), "inmem") {
		workerFactory := func(node cluster.Node) workerapi.Worker {
			wg := &sync.WaitGroup{}
			exec := fakeexecer.NewSimExecer(wg)
			r := localrunner.NewSimpleRunner(exec)
			return runnerworker.MakeWorker(r)
		}
		return cl, workerFactory
	} else {
		protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
		transportFactory := thrift.NewTTransportFactory()
		workerFactory := func(node cluster.Node) workerapi.Worker {
			return client.NewClient(transportFactory, protocolFactory, string(node.Id()))
		}
		return cl, workerFactory
	}
}
