package cluster

import (
	"fmt"
	"time"
)

type FetchCron struct {
	Ticker  *time.Ticker
	f 	    Fetcher
	Cl 		*Cluster
}

// Returns a full list of visible nodes.
type Fetcher interface {
	Fetch() ([]Node, error)
}

func NewFetchCron(f Fetcher, t time.Duration, cl *Cluster) *FetchCron {
	c := &FetchCron{
		Ticker: time.NewTicker(t),
		f: 		f,
		Cl: 	cl,
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
		c.Cl.stateCh <- nodes
	}
}
