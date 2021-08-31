package cluster

import (
	"time"
)

type fetchCron struct {
	freq           time.Duration
	f              Fetcher
	fetchedNodesCh chan []Node
}

// Defines the way in which a full set of Nodes in a Cluster is retrieved
type Fetcher interface {
	Fetch() ([]Node, error)
}

// Given a Fetcher implementation and a Ticker, start a ticker loop that
// fetches the current nodes and updates cluster's latestNodeList with this list
func StartFetchCron(f Fetcher, freq time.Duration, fetchBufferSize int) chan []Node {
	c := &fetchCron{
		freq:           freq,
		f:              f,
		fetchedNodesCh: make(chan []Node, fetchBufferSize),
	}
	go c.loop()
	return c.fetchedNodesCh
}

func (c *fetchCron) loop() {
	tickCh := time.NewTicker(c.freq)
	for range tickCh.C {
		nodes, err := c.f.Fetch()
		if err != nil {
			// TODO(rcouto): Correctly handle as many errors as possible
			continue
		}
		c.fetchedNodesCh <- nodes
	}
}
