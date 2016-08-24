package scootconfig

import (
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/local"
	clusterimpl "github.com/scootdev/scoot/cloud/cluster/memory"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/sched/queue"
	queueimpl "github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/sched/worker/fake"
	"github.com/scootdev/scoot/sched/worker/rpc"
)

type QueueMemoryConfig struct {
	Type     string
	Capacity int
}

func (c *QueueMemoryConfig) Install(e *ice.MagicBag) {
	e.Put(func() queue.Queue {
		return queueimpl.NewSimpleQueue(c.Capacity)
	})
}

type ClusterMemoryConfig struct {
	Type  string
	Count int
}

func (c *ClusterMemoryConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *ClusterMemoryConfig) Create() (cluster.Cluster, error) {
	workerNodes := make([]cluster.Node, c.Count)
	for i := 0; i < c.Count; i++ {
		workerNodes[i] = clusterimpl.NewIdNode(fmt.Sprintf("inmemory%d", i))
	}
	return clusterimpl.NewCluster(workerNodes, nil), nil
}

type ClusterLocalConfig struct {
	Type string
}

func (c *ClusterLocalConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *ClusterLocalConfig) Create() (cluster.Cluster, error) {
	sub := local.Subscribe()
	return clusterimpl.NewCluster(sub.InitialMembers, sub.Updates), nil
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
