package cluster

import (
	"sort"
)

// Cluster represents a cluster of Nodes.
// type Cluster interface {
// 	// Members returns the current members, or an error if they can't be determined.
// 	Members() []Node
// 	// Subscribe subscribes to changes to the cluster.
// 	Subscribe() Subscriber
// 	// Stop monitoring this cluster
// 	Close() error
// }

type Cluster struct {
	State  		*State
	reqCh   	chan interface{}
	updateCh 	chan []NodeUpdate
	stateCh 	chan []Node
	subs    	[]chan []NodeUpdate
}

func NewCluster(state []Node, updateCh chan []NodeUpdate, stateCh chan []Node) *Cluster {
	s := MakeState(state)
	// s.SetAndDiff(state)
	c := &Cluster{
		State:  	s,
		reqCh:   	make(chan interface{}),
		updateCh:	updateCh,
		stateCh: 	stateCh,
		subs:    	nil,
	}
	go c.loop()
	return c
}

func (c *Cluster) Members() []Node {
	ch := make(chan []Node)
	c.reqCh <- ch
	return <-ch
}

func (c *Cluster) Subscribe() Subscriber {
	ch := make(chan Subscriber)
	c.reqCh <- ch
	return <-ch
}

func (c *Cluster) Close() error {
	close(c.reqCh)
	return nil
}

func (c *Cluster) done() bool {
	return c.updateCh == nil && c.stateCh == nil && c.reqCh == nil
}

func (c *Cluster) loop() {
	for !c.done() {
		select {
		case updates, ok := <-c.updateCh:
			if !ok {
				c.updateCh = nil
				continue
			}
			c.State.Update(updates)
			for _, sub := range c.subs {
				sub <- updates
			}
		case nodes, ok := <-c.stateCh:
			if !ok {
				c.stateCh = nil
				continue
			}
			outgoing := c.State.SetAndDiff(nodes)
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
		req <- c.Current()
	case chan Subscriber:
		// Subscribe()
		ch := make(chan []NodeUpdate)
		s := newSubscriber(c.Current(), c, ch)
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

func (c *Cluster) closeSubscription(s *Subscriber) {
	c.reqCh <- s.inCh
}

func (c *Cluster) Current() []Node {
	var r []Node
	for _, v := range c.State.Nodes {
		r = append(r, v)
	}
	sort.Sort(NodeSorter(r))
	return r
}