package cluster

import (
	"sort"
	"time"
)

// Cluster represents a cluster of Nodes.

type Cluster struct {
	state    *state
	reqCh    chan interface{}
	ch       chan interface{}
	updateCh chan []NodeUpdate
	subs     []chan []NodeUpdate
}

// Cluster's ch channel accepts []Node and []NodeUpdate types, which then
// get passed to its state to either SetAndDiff or UpdateAndFilter

func NewCluster(state []Node, updateCh chan []NodeUpdate, ch chan interface{}, fetcher Fetcher) *Cluster {
	s := makeState(state)
	c := &Cluster{
		state:    s,
		reqCh:    make(chan interface{}),
		ch:       ch,
		updateCh: updateCh,
		subs:     nil,
	}
	makeFetchCron(fetcher, time.Millisecond, ch)
	go c.loop()
	return c
}

func (c *Cluster) Members() []Node {
	ch := make(chan []Node)
	c.reqCh <- ch
	return <-ch
}

func (c *Cluster) Subscribe() Subscription {
	ch := make(chan Subscription)
	c.reqCh <- ch
	return <-ch
}

func (c *Cluster) Close() error {
	close(c.reqCh)
	return nil
}

func (c *Cluster) done() bool {
	return c.ch == nil && c.reqCh == nil && c.updateCh == nil
}

func (c *Cluster) loop() {
	for !c.done() {
		select {
		case updates, ok := <-c.updateCh:
			if !ok {
				c.updateCh = nil
				continue
			}
			c.ch <- updates
			continue
		case nodesOrUpdates, ok := <-c.ch:
			if !ok {
				c.ch = nil
				continue
			}
			outgoing := []NodeUpdate{}
			if updates, ok := nodesOrUpdates.([]NodeUpdate); ok {
				outgoing = c.state.filterAndUpdate(updates)
			} else if nodes, ok := nodesOrUpdates.([]Node); ok {
				sort.Sort(NodeSorter(nodes))
				outgoing = c.state.setAndDiff(nodes)
			}
			for _, sub := range c.subs {
				sub <- outgoing
			}
		case req, ok := <-c.reqCh:
			if !ok {
				c.reqCh = nil
				continue
			}
			c.handleReq(req)
		}
	}
	for _, sub := range c.subs {
		close(sub)
	}
}

func (c *Cluster) handleReq(req interface{}) {
	switch req := req.(type) {
	case chan []Node:
		// Members()
		req <- c.current()
	case chan Subscription:
		// Subscribe()
		ch := make(chan []NodeUpdate)
		s := makeSubscription(c.current(), c, ch)
		c.subs = append(c.subs, ch)
		req <- s
	case chan []NodeUpdate:
		// close of a subscription
		for i, sub := range c.subs {
			if sub == req {
				c.subs = append(
					c.subs[0:i],
					c.subs[i+1:]...)
				close(req)
				break
			}
		}
	}
}

func (c *Cluster) closeSubscription(s *subscriber) {
	c.reqCh <- s.inCh
}

func (c *Cluster) current() []Node {
	var r []Node
	for _, v := range c.state.nodes {
		r = append(r, v)
	}
	sort.Sort(NodeSorter(r))
	return r
}
