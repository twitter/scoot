package cluster

import (
	"fmt"
	"time"
)

type FetchCron struct {
	Ticker *time.Ticker
	f      Fetcher
	Ch     chan []Node
}

// Returns a full list of visible nodes.
type Fetcher interface {
	Fetch() ([]Node, error)
}

func NewFetchCron(f Fetcher, t time.Duration, ch chan []Node) *FetchCron {
	c := &FetchCron{
		Ticker: time.NewTicker(t),
		f:      f,
		Ch:     ch,
	}
	go c.loop()
	return c
}

func (c *FetchCron) loop() {
	for _ = range c.Ticker.C {
		nodes, err := c.f.Fetch()
		if err != nil {
			// Log? Return?
			fmt.Println("Received error: %v", err)
		}
		c.Ch <- nodes
	}
}
