package cluster

import (
	"sort"
	"time"
)

// Returns a full list of visible nodes.
type Fetcher interface {
	Fetch() ([]Node, error)
}

// Subscribe subscribes to node updates for the specified service
func Subscribe(fetcher Fetcher) Subscription {
	ticker := time.NewTicker(time.Duration(15 * time.Second))
	ch := MakeDiffSubscription(ticker.C, fetcher)
	return Subscription{nil, ch, &closer{ticker}}
}

type closer struct {
	ticker *time.Ticker
}

func (c *closer) Close() error {
	c.ticker.Stop()
	return nil
}

func MakeDiffSubscription(ticker <-chan time.Time, fetcher Fetcher) chan []NodeUpdate {
	c := diffSubscription{
		fetcher:  fetcher,
		nodes:    make(map[string]Node),
		tickCh:   ticker,
		fetchCh:  nil,
		outCh:    make(chan []NodeUpdate),
		outgoing: nil,
	}
	go c.loop()
	return c.outCh
}

// diffSubscription fetches the list of nodes periodically, diffs the list to determine
// what has changes, and sends updates.
type diffSubscription struct {
	// channel for ticks to suggest an update
	tickCh <-chan time.Time

	fetcher Fetcher
	// the current view of our nodes
	nodes map[string]Node
	// result channel for in-progress fetch
	fetchCh chan nodesAndError

	// channel we send node updates to
	outCh chan []NodeUpdate
	// NodeUpdates that are ready and waiting to send
	outgoing []NodeUpdate
}

func (c *diffSubscription) loop() {
	// Fetch and Parse to begin with; don't wait for a whole iteration
	c.fetchCh = make(chan nodesAndError)
	go func() {
		nodes, err := c.fetcher.Fetch()
		c.fetchCh <- nodesAndError{nodes, err}
	}()
	for c.tickCh != nil || c.fetchCh != nil || len(c.outgoing) > 0 {
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
			if c.fetchCh != nil {
				// We're already waiting for a fetch, ignore this tick
				continue
			}
			c.fetchCh = make(chan nodesAndError)
			go func() {
				nodes, err := c.fetcher.Fetch()
				c.fetchCh <- nodesAndError{nodes, err}
			}()
		case r := <-c.fetchCh:
			c.fetchCh = nil
			c.handleFetch(r.nodes, r.err)
		case outCh <- c.outgoing:
			c.outgoing = nil
		}
	}
	close(c.outCh)
}

func (c *diffSubscription) handleFetch(nodes []Node, err error) {
	if err != nil {
		// TODO(dbentley): what should we do if we keep getting errors?
		// Log? Publish Stats? Have some other channel to signal?
		return
	}
	next := make(map[string]Node)
	added := []Node{}
	for _, n := range nodes {
		id := string(n.Id())
		next[id] = n
		if _, exists := c.nodes[id]; exists {
			delete(c.nodes, id)
		} else {
			added = append(added, n)
		}
	}

	for _, n := range added {
		c.outgoing = append(c.outgoing, NodeUpdate{
			UpdateType: NodeAdded,
			Id:         n.Id(),
			Node:       n,
		})
	}

	// At this point, any nodes left in c.nodes were not in the next nodes,
	// and thus have been removed in this update.
	removed := []Node{}
	for _, n := range c.nodes {
		removed = append(removed, n)
	}

	// Now sort the removed nodes so we deliver them in a predictable order.
	sort.Sort(Nodes(removed))

	for _, n := range removed {
		c.outgoing = append(c.outgoing, NodeUpdate{
			UpdateType: NodeRemoved,
			Id:         n.Id(),
		})
	}

	// TODO(dbentley): we may have outgoing messages that are now stale.
	// e.g., if a node flapped, we will end up with two messages in outgoing:
	// one to remove it and then one to add it back.
	// We should check outgoing for such messages and remove both.

	// Now use the map created from this update as our current view of the world
	c.nodes = next
}

// Single return type for use in channels.
type nodesAndError struct {
	nodes []Node
	err   error
}
