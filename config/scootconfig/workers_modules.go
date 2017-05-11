package scootconfig

import (
	"time"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/sched/worker/workers"
	"github.com/scootdev/scoot/workerapi/client"
	"github.com/scootdev/thrift/lib/go/thrift"
)

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
	pf thrift.TProtocolFactory) (func(cluster.Node) runner.Service, error) {

	pollingPeriod := defaultPollingPeriod
	var err error

	// apply defaults
	if c.PollingPeriod != "" {
		pollingPeriod, err = time.ParseDuration(c.PollingPeriod)
		if err != nil {
			return nil, err
		}
	}

	rf := func(node cluster.Node) runner.Service {
		di := dialer.NewSimpleDialer(tf, pf)
		cl, _ := client.NewSimpleClient(di, string(node.Id()))
		return runners.NewPollingService(cl, cl, cl, pollingPeriod)
	}

	return rf, nil
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
	bag.Put(func(tmp *temp.TempDir) func(cluster.Node) runner.Service {
		return InmemoryWorkerFactory(tmp)
	})
}

func InmemoryWorkerFactory(tmp *temp.TempDir) func(cluster.Node) runner.Service {
	return func(node cluster.Node) runner.Service {
		return workers.MakeInmemoryWorker(node, tmp)
	}
}
