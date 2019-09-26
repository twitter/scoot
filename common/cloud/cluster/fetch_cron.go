package cluster

import (
	"time"
)

type fetchCron struct {
	tickCh <-chan time.Time
	f      Fetcher
	outCh  chan ClusterUpdate
}

// Defines the way in which a full set of Nodes in a Cluster is retrieved
type Fetcher interface {
	Fetch() ([]Node, error)
}

// Given a Fetcher implementation and a Ticker, returns a channel over which
// ClusterUpdates will be sent to periodically from a new Goroutine
func MakeFetchCron(f Fetcher, tickCh <-chan time.Time) chan ClusterUpdate {
	outCh := make(chan ClusterUpdate)
	c := &fetchCron{
		tickCh: tickCh,
		f:      f,
		outCh:  outCh,
	}
	go c.loop()
	return outCh
}

func (c *fetchCron) loop() {
	for range c.tickCh {
		nodes, err := c.f.Fetch()
		if err != nil {
			// TODO(rcouto): Correctly handle as many errors as possible
			continue
		}
		c.outCh <- nodes
	}
	close(c.outCh)
}

// TODO(rcouto): add close and shutdown
