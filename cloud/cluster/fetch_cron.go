package cluster

import (
	"time"
)

type fetchCron struct {
	tickCh *time.Ticker
	f      Fetcher
	outCh  chan interface{}
	closer chan struct{}
}

// Returns a full list of visible nodes.
type Fetcher interface {
	Fetch() ([]Node, error)
}

func makeFetchCron(f Fetcher, t time.Duration, ch chan interface{}) *fetchCron {
	c := &fetchCron{
		tickCh: time.NewTicker(t),
		f:      f,
		outCh:  ch,
		closer: make(chan struct{}),
	}
	go c.loop()
	return c
}

func (c *fetchCron) loop() {
	for range c.tickCh.C {
		nodes, err := c.f.Fetch()
		if err != nil {
			// TODO(rcouto): Correctly handle as many errors as possible
			return
		}
		c.outCh <- nodes
	}
	close(c.outCh)
}

// TODO(rcouto): add close and shutdown
