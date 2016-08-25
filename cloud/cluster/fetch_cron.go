package cluster

import (
	"time"
)

type FetchCron struct {
	tickCh   <-chan time.Time
	f        Fetcher
	inCh     chan nodesAndError
	outCh    chan []NodeUpdate
	outgoing []NodeUpdate
	closer   chan struct{}
	cl       *Cluster
}

// Returns a full list of visible nodes.
type Fetcher interface {
	Fetch() ([]Node, error)
}

func NewFetchCron(f Fetcher, t time.Duration, cl *Cluster) *FetchCron {
	c := &FetchCron{
		tickCh:   time.NewTicker(t).C,
		f:        f,
		inCh:     nil,
		outCh:    make(chan []NodeUpdate),
		outgoing: nil,
		closer:   make(chan struct{}),
		cl:       cl,
	}
	go c.loop()
	return c
}

func (c *FetchCron) loop() {
	c.inCh = make(chan nodesAndError)
	go func() {
		nodes, err := c.f.Fetch()
		c.inCh <- nodesAndError{nodes, err}
	}()
	for c.tickCh != nil || c.inCh != nil || len(c.outgoing) > 0 {
		var outCh chan []NodeUpdate
		if len(c.outgoing) > 0 {
			outCh = c.outCh
		}
		select {
		case _, ok := <-c.tickCh:
			if !ok {
				// tickCh is closed means we should stop
				c.tickCh = nil
			}
			if c.inCh != nil {
				// We're already waiting for a fetch, ignore this tick
				continue
			}
			c.inCh = make(chan nodesAndError)
			go func() {
				nodes, err := c.f.Fetch()
				c.inCh <- nodesAndError{nodes, err}
			}()
		case r := <-c.inCh:
			c.inCh = nil
			c.handleFetch(r.nodes, r.err)
		case outCh <- c.outgoing:
			c.outgoing = nil
		}
	}
	close(c.outCh)
}

func (c *FetchCron) handleFetch(nodes []Node, err error) {
	if err != nil {
		// TODO(rcouto): Correctly handle as many errors as possible
		return
	}
	c.cl.ch <- nodes
}

func (c *FetchCron) Close() {
	c.closer <- struct{}{}
}

type nodesAndError struct {
	nodes []Node
	err   error
}
