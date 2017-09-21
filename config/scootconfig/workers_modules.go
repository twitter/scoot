package scootconfig

import (
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/sched/worker/workers"
	"github.com/twitter/scoot/workerapi/client"
)

type ClientTimeout time.Duration

const DefaultClientTimeout = time.Minute

// Parameters for configuring connections to remote (Thrift) workers.
type WorkersThriftConfig struct {
	Type          string
	PollingPeriod string // will be parsed to a time.Duration
}

const defaultPollingPeriod = time.Duration(250) * time.Millisecond

func (c *WorkersThriftConfig) Create(
	tf thrift.TTransportFactory,
	pf thrift.TProtocolFactory,
	ct ClientTimeout) (func(cluster.Node) runner.Service, error) {

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
		di := dialer.NewSimpleDialer(tf, pf, time.Duration(ct))
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
