package client

import (
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/wisechengyi/scoot/cloud/cluster"
	"github.com/wisechengyi/scoot/common/dialer"
	"github.com/wisechengyi/scoot/runner"
	"github.com/wisechengyi/scoot/runner/runners"
)

const defaultPollingPeriod = time.Duration(250) * time.Millisecond

func NewWorkerThriftClient(
	tf thrift.TTransportFactory,
	pf thrift.TProtocolFactory,
	ct time.Duration,
	wc WorkersClientJSONConfig) (func(cluster.Node) runner.Service, error) {

	pollingPeriod := defaultPollingPeriod
	var err error

	// apply defaults
	if wc.PollingPeriod != "" {
		pollingPeriod, err = time.ParseDuration(wc.PollingPeriod)
		if err != nil {
			return nil, err
		}
	}

	rf := func(node cluster.Node) runner.Service {
		di := dialer.NewSimpleDialer(tf, pf, time.Duration(ct))
		cl, _ := NewSimpleClient(di, string(node.Id()))
		return runners.NewPollingService(cl, cl, pollingPeriod)
	}

	return rf, nil
}
