package memory

import (
	"github.com/scootdev/scoot/cloud/cluster"
)

func NewCluster(initial []cluster.Node, updateCh chan []cluster.NodeUpdate) cluster.Cluster {
	c := &simpleCluster{
		inCh:    updateCh,
		reqCh:   make(chan interface{}),
		members: State{members: initial},
		subs:    nil,
	}
	go c.loop()
	return c
}

type simpleCluster struct {
	inCh  chan []cluster.NodeUpdate
	reqCh chan interface{}

	members State
	subs    []chan []cluster.NodeUpdate
}

func (c *simpleCluster) Members() ([]cluster.Node, error) {
	ch := make(chan []cluster.Node)
	c.reqCh <- ch
	return <-ch, nil
}

func (c *simpleCluster) Subscribe() (cluster.Subscription, error) {
	ch := make(chan cluster.Subscription)
	c.reqCh <- ch
	return <-ch, nil
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
			c.members.Apply(updates)
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
		req <- c.members.Current()
	case chan cluster.Subscription:
		// Subscribe()
		ch := make(chan []cluster.NodeUpdate)
		s := makeSubscription(c.members.Current(), c, ch)
		c.subs = append(c.subs, ch)
		req <- s
	case chan []cluster.NodeUpdate:
		// subscription.Close()
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

// State holds the state of a Cluster (which is just its members)
type State struct {
	members []cluster.Node
}

func (s *State) Apply(updates []cluster.NodeUpdate) {
	// TODO(dbentley): this could be slow if there are lots of updates
	for _, update := range updates {
		switch update.UpdateType {
		case cluster.NodeAdded:
			s.members = append(s.members, update.Node)
		case cluster.NodeRemoved:
			for i, n := range s.members {
				if n.Id() == update.Id {
					s.members = append(s.members[0:i], s.members[i+1:]...)
				}
			}
		}
	}
}

func (s *State) Current() []cluster.Node {
	// Defensive copy
	return append([]cluster.Node{}, s.members...)
}
