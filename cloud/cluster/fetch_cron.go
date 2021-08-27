package cluster

import (
	"time"
)

type fetchCron struct {
	tickCh  <-chan time.Time
	f       Fetcher
	cluster Cluster
}

// Defines the way in which a full set of Nodes in a Cluster is retrieved
type Fetcher interface {
	Fetch() ([]Node, error)
}

// Given a Fetcher implementation and a Ticker, start a ticker loop that
// fetches the current nodes and updates cluster's latestNodeList with this list
func MakeFetchCron(f Fetcher, tickCh <-chan time.Time, cluster Cluster) {
	c := &fetchCron{
		tickCh:  tickCh,
		f:       f,
		cluster: cluster,
	}
	go c.loop()
}

func (c *fetchCron) loop() {
	for range c.tickCh {
		nodes, err := c.f.Fetch()
		if err != nil {
			// TODO(rcouto): Correctly handle as many errors as possible
			continue
		}
		c.cluster.SetLatestNodesList(nodes)
	}
}
