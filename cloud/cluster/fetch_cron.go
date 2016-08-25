package cluster

import (
	"time"
)

type fetchCron struct {
	tickCh <-chan time.Time
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
		tickCh: time.NewTicker(t).C,
		f:      f,
		outCh:  ch,
		closer: make(chan struct{}),
	}
	go c.loop()
	return c
}

func (c *fetchCron) loop() {
	for c.tickCh != nil {
		select {
		case _, ok := <-c.tickCh:
			if !ok {
				// tickCh is closed means we should stop
				c.tickCh = nil
			}
			nodes, err := c.f.Fetch()
			c.handleFetch(nodes, err)
		case _, ok := <-c.closer:
			if !ok {
				continue
			} else {
				return
			}
		}
	}
	close(c.outCh)
}

func (c *fetchCron) handleFetch(nodes []Node, err error) {
	if err != nil {
		// TODO(rcouto): Correctly handle as many errors as possible
		return
	}
	c.outCh <- nodes
}

func (c *fetchCron) close() {
	c.closer <- struct{}{}
}

type nodesAndError struct {
	nodes []Node
	err   error
}
