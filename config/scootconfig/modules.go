package scootconfig

import (
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/local"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/sched/worker/fake"
	"github.com/scootdev/scoot/sched/worker/rpc"
)

type ClusterMemoryConfig struct {
	Type  string
	Count int
}

func (c *ClusterMemoryConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *ClusterMemoryConfig) Create() (*cluster.Cluster, error) {
	workerNodes := make([]cluster.Node, c.Count)
	for i := 0; i < c.Count; i++ {
		workerNodes[i] = cluster.NewIdNode(fmt.Sprintf("inmemory%d", i))
	}
	return cluster.NewCluster(workerNodes, nil), nil
}

type ClusterLocalConfig struct {
	Type string
}

func (c *ClusterLocalConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *ClusterLocalConfig) Create() (*cluster.Cluster, error) {
	f := local.MakeFetcher()
	updates := cluster.MakeFetchCron(f, time.NewTicker(time.Second).C)
	return cluster.NewCluster(nil, updates), nil
}

type WorkersThriftConfig struct {
	Type string
}

func MakeRpcWorkerFactory(tf thrift.TTransportFactory, pf thrift.TProtocolFactory) worker.WorkerFactory {
	return func(node cluster.Node) worker.Worker {
		return rpc.NewThriftWorker(tf, pf, string(node.Id()))
	}
}

func (c *WorkersThriftConfig) Install(bag *ice.MagicBag) {
	bag.Put(MakeRpcWorkerFactory)
}

type WorkersLocalConfig struct {
	Type string
	// TODO(dbentley): allow specifying what the runner/execer underneath this local worker is like
}

func (c *WorkersLocalConfig) Install(bag *ice.MagicBag) {
	bag.Put(func() worker.WorkerFactory {
		return fake.MakeWaitingNoopWorker
	})
}
