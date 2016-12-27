// Scootconfig provides modules for creating and initializing configs
// for Scoot components within Setup Cloud Scoot, via Ice.
// Modules include a type that defines what fields make up the config,
// and Install and Create methods that are used by Ice to initialize
// a var of the type. See scoot/ice for more info on the Ice
// dependency injection system.
package scootconfig

import (
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/local"
	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/saga/sagalogs"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/sched/worker/workers"
	"github.com/scootdev/scoot/workerapi/client"
)

// InMemorySagaLog struct is used by goice to create an InMemory instance
// of the SagaLog interface.
type InMemorySagaLogConfig struct {
	Type string
}

// Adds the InMemorySagaLog Create function to the goice MagicBag
func (c *InMemorySagaLogConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

// Creates an instance of an InMemorySagaLog
func (c *InMemorySagaLogConfig) Create() saga.SagaLog {
	return sagalogs.MakeInMemorySagaLog()
}

// Parameters to configure the Stateful Scheduler
// MaxRetriesPerTask - the number of times to retry a failing task before
// 										 marking it as completed.
// DebugMode - if true, starts the scheduler up but does not start
// 						 the update loop.  Instead the loop must be advanced manulaly
//             by calling step()
// RecoverJobsOnStartup - if true, the scheduler recovers active sagas,
//             from the sagalog, and restarts them.
type StatefulSchedulerConfig struct {
	Type                 string
	MaxRetriesPerTask    int
	DebugMode            bool
	RecoverJobsOnStartup bool
}

func (c *StatefulSchedulerConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *StatefulSchedulerConfig) Create() scheduler.SchedulerConfig {
	return scheduler.SchedulerConfig{
		MaxRetriesPerTask: c.MaxRetriesPerTask,
	}
}

// Parameters for configuring an in-memory Scoot cluster
// Count - number of in-memory workers
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

// Parameters for configuring a Scoot cluster that will have locally-run components.
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

// Parameters for configuring connections to remote (Thrift) workers.
type WorkersThriftConfig struct {
	Type               string
	PollingPeriod      string // will be parsed to a time.Duration
	EnforceTaskTimeout bool
	TaskTimeout        string // will be parsed to a time.Duration
}

const defaultPollingPeriod = time.Duration(250) * time.Millisecond
const defaultTaskTimeout = time.Duration(30) * time.Minute
const defaultOverhead = time.Duration(5) * time.Minute

func (c *WorkersThriftConfig) Create(
	tf thrift.TTransportFactory,
	pf thrift.TProtocolFactory) (worker.WorkerFactory, error) {

	pollingPeriod := defaultPollingPeriod
	taskTimeout := defaultTaskTimeout
	var err error

	// apply defaults
	if c.PollingPeriod != "" {
		pollingPeriod, err = time.ParseDuration(c.PollingPeriod)
		if err != nil {
			return nil, err
		}
	}

	if c.TaskTimeout != "" {
		taskTimeout, err = time.ParseDuration(c.TaskTimeout)
		if err != nil {
			return nil, err
		}
	}

	wf := func(node cluster.Node) worker.Worker {
		di := dialer.NewSimpleDialer(tf, pf)
		cl, _ := client.NewSimpleClient(di, string(node.Id()))
		q := runners.NewPollingService(cl, cl, cl, pollingPeriod)
		return workers.NewServiceWorker(q, taskTimeout, defaultOverhead)
	}

	return wf, nil
}

func (c *WorkersThriftConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

// Parameters for configuring locally started workers
type WorkersLocalConfig struct {
	Type string
	// TODO(dbentley): allow specifying what the runner/execer underneath this local worker is like
}

func (c *WorkersLocalConfig) Install(bag *ice.MagicBag) {
	bag.Put(func() worker.WorkerFactory {
		return workers.MakeInmemoryWorker
	})
}
