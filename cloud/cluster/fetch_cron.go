package cluster

import (
	"log"
	"time"
)

type FetchCron struct {
	ticker *time.Ticker
	f      Fetcher
	Ch     chan []Node
	closer chan struct{}
}

// Returns a full list of visible nodes.
type Fetcher interface {
	Fetch() ([]Node, error)
}

func NewFetchCron(f Fetcher, t time.Duration, ch chan []Node) *FetchCron {
	c := &FetchCron{
		ticker: time.NewTicker(t),
		f:      f,
		Ch:     ch,
		closer: make(chan struct{}),
	}
	go c.loop()
	return c
}

func (c *FetchCron) loop() {
	defer close(c.closer)
	for {
		select {
		case <-c.ticker.C:
			nodes, err := c.f.Fetch()
			if err != nil {
				// TODO(rcouto): Correctly handle as many errors as possible
				log.Printf("Received error: %v", err)
			}
			c.Ch <- nodes
		case <-c.closer:
			c.ticker.Stop()
			return
		}
	}
}

func (c *FetchCron) Close() {
	c.closer <- struct{}{}
}
