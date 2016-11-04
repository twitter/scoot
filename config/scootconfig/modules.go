package scootconfig

import (
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/local"
	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/sched/worker/workers"
	"github.com/scootdev/scoot/workerapi/client"
)

type StatefulSchedulerConfig struct {
	Type              string
	MaxRetriesPerTask int
}

func (c *StatefulSchedulerConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *StatefulSchedulerConfig) Create() scheduler.SchedulerConfig {
	return scheduler.SchedulerConfig{
		MaxRetriesPerTask: c.MaxRetriesPerTask,
	}
}

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
	Type               string
	PollingPeriod      string // will be parsed to a time.Duration
	EnforceTaskTimeout bool
	TaskTimeout        string // will be parsed to a time.Duration
}

const defaultPollingPeriod = time.Duration(250) * time.Millisecond
const defaultTaskTimeout = time.Duration(30) * time.Minute

func (c *WorkersThriftConfig) Create(
	tf thrift.TTransportFactory,
	pf thrift.TProtocolFactory) (worker.WorkerFactory, error) {

	pp := defaultPollingPeriod
	tt := defaultTaskTimeout
	var err error

	// apply defaults
	if c.PollingPeriod != "" {
		pp, err = time.ParseDuration(c.PollingPeriod)
		if err != nil {
			return nil, err
		}
	}

	if c.TaskTimeout != "" {
		tt, err = time.ParseDuration(c.TaskTimeout)
		if err != nil {
			return nil, err
		}
	}

	wf := func(node cluster.Node) worker.Worker {
		di := dialer.NewSimpleDialer(tf, pf)
		cl, _ := client.NewSimpleClient(di, string(node.Id()))
		return workers.NewPollingWorkerWithTimeout(cl, pp, c.EnforceTaskTimeout, tt)
	}

	return wf, nil
}

func (c *WorkersThriftConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

type WorkersLocalConfig struct {
	Type string
	// TODO(dbentley): allow specifying what the runner/execer underneath this local worker is like
}

func (c *WorkersLocalConfig) Install(bag *ice.MagicBag) {
	bag.Put(func() worker.WorkerFactory {
		return workers.MakeInmemoryWorker
	})
}
