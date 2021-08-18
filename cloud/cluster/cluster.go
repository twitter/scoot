// Cluster provides the means for coordinating the schedulers and workers that
// make up a Scoot system. This is achieved mainly through the Cluster type,
// individual Nodes, and Subscriptions to cluster changes.
package cluster

import (
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twitter/scoot/common/stats"
)

// Cluster represents a group of Nodes and has mechanisms for receiving updates.
type Cluster struct {
	state              *state
	reqCh              chan interface{}
	updateCh           chan ClusterUpdate
	subs               []chan []NodeUpdate
	priorIterationTime time.Time
	Stats              stats.StatsReceiver
}

// Clusters can be updated in two ways:
// *) a new state of the Cluster, which is a []Node
// *) updates to specific Nodes, which is a []NodeUpdate
type ClusterUpdate interface{}

// Cluster's ch channel accepts []Node and []NodeUpdate types, which then
// get passed to its state to either SetAndDiff or UpdateAndFilter
func NewCluster(state []Node, updateCh chan ClusterUpdate) *Cluster {
	s := makeState(state)
	c := &Cluster{
		state:              s,
		reqCh:              make(chan interface{}),
		updateCh:           updateCh,
		subs:               nil,
		priorIterationTime: time.Now(),
	}
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
	return c.reqCh == nil && c.updateCh == nil
}

func (c *Cluster) loop() {
	for !c.done() {
		select {
		case nodesOrUpdates, ok := <-c.updateCh:
			if !ok {
				c.updateCh = nil
				continue
			}
			outgoing := []NodeUpdate{}
			if updates, ok := nodesOrUpdates.([]NodeUpdate); ok {
				outgoing = c.state.filterAndUpdate(updates)
			} else if nodes, ok := nodesOrUpdates.([]Node); ok {
				sort.Sort(NodeSorter(nodes))
				outgoing = c.state.setAndDiff(nodes)
			}
			elapsed := time.Since(c.priorIterationTime)
			log.Infof("putting changes on channel, time since last iteration: %s", time.Since(c.priorIterationTime))
			if c.Stats != nil {
				c.Stats.Gauge(stats.ClusterTimeSinceLastUpdate_ms).Update(elapsed.Milliseconds())
			}
			c.priorIterationTime = time.Now()
			for _, sub := range c.subs {
				sub <- outgoing
			}
			log.Info("changes are in the channel")
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
