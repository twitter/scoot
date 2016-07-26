package memory

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"sort"
)

func NewCluster(initial []cluster.Node, updateCh chan []cluster.NodeUpdate) cluster.Cluster {
	members := make(map[cluster.NodeId]cluster.Node)
	for _, n := range initial {
		members[n.Id()] = n
	}
	c := &simpleCluster{
		inCh:    updateCh,
		reqCh:   make(chan interface{}),
		members: members,
		subs:    nil,
	}
	go c.loop()
	return c
}

type simpleCluster struct {
	inCh  chan []cluster.NodeUpdate
	reqCh chan interface{}

	members map[cluster.NodeId]cluster.Node
	subs    []chan []cluster.NodeUpdate
}

func (c *simpleCluster) Members() []cluster.Node {
	ch := make(chan []cluster.Node)
	c.reqCh <- ch
	return <-ch
}

func (c *simpleCluster) Subscribe() cluster.Subscription {
	ch := make(chan cluster.Subscription)
	c.reqCh <- ch
	return <-ch
}

func (c *simpleCluster) Close() error {
	close(c.reqCh)
	return nil
}

func (c *simpleCluster) done() bool {
	return c.inCh == nil && c.reqCh == nil
}

func (c *simpleCluster) loop() {
	for !c.done() {
		select {
		case updates, ok := <-c.inCh:
			if !ok {
				c.inCh = nil
				continue
			}
			c.apply(updates)
			for _, sub := range c.subs {
				sub <- updates
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

func (c *simpleCluster) handleReq(req interface{}) {
	switch req := req.(type) {
	case chan []cluster.Node:
		// Members()
		req <- c.current()
	case chan cluster.Subscription:
		// Subscribe()
		ch := make(chan []cluster.NodeUpdate)
		s := makeSubscription(c.current(), c, ch)
		c.subs = append(c.subs, ch)
		req <- s
	case chan []cluster.NodeUpdate:
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

func (c *simpleCluster) closeSubscription(s *subscriber) {
	c.reqCh <- s.inCh
}

func (c *simpleCluster) apply(updates []cluster.NodeUpdate) {
	for _, update := range updates {
		switch update.UpdateType {
		case cluster.NodeAdded:
			c.members[update.Id] = update.Node
		case cluster.NodeRemoved:
			delete(c.members, update.Id)
		}
	}
}

func (c *simpleCluster) current() []cluster.Node {
	var r []cluster.Node
	for _, v := range c.members {
		r = append(r, v)
	}
	sort.Sort(nodeSorter(r))
	return r
}

type nodeSorter []cluster.Node

func (n nodeSorter) Len() int           { return len(n) }
func (n nodeSorter) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n nodeSorter) Less(i, j int) bool { return n[i].Id() < n[j].Id() }
