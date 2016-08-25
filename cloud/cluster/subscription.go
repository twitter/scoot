package cluster

import (
	"io"
	"time"
)

// Subscriber receives node updates from its cluster and maintains a queue
// so that the sender doesn't have to worry about send blocking

// Subscription is a subscription to cluster changes.
type Subscription struct {
	InitialMembers []Node            // The members at the time the subscription started
	Updates        chan []NodeUpdate // Updates as they happen
	Closer         io.Closer         // How to stop subscribing
}

type subscriber struct {
	inCh  chan []NodeUpdate
	outCh chan []NodeUpdate
	cl    *Cluster
	queue []NodeUpdate
}

type closer struct {
	ticker *time.Ticker
}

func Subscribe(fetcher Fetcher) Subscription {
	ticker := time.NewTicker(time.Duration(15 * time.Second))
	return Subscription{nil, nil, &closer{ticker}}
}

func makeSubscription(initial []Node, cl *Cluster, inCh chan []NodeUpdate) Subscription {
	s := &subscriber{
		inCh:  inCh,
		outCh: make(chan []NodeUpdate),
		cl:    cl,
		queue: nil,
	}
	go s.loop()
	return Subscription{
		InitialMembers: initial,
		Updates:        s.outCh,
		Closer:         s,
	}
}

func (s *subscriber) Close() error {
	s.cl.closeSubscription(s)
	return nil
}

func (s *subscriber) loop() {
	for s.inCh != nil || len(s.queue) > 0 {
		var outCh chan []NodeUpdate
		var outgoing []NodeUpdate
		if len(s.queue) > 0 {
			outCh = s.outCh
			outgoing = s.queue
		}
		select {
		case updates, ok := <-s.inCh:
			if !ok {
				s.inCh = nil
				continue
			}
			s.queue = append(s.queue, updates...)
		case outCh <- outgoing:
			s.queue = nil
		}
	}
	close(s.outCh)
}

func (c *closer) Close() error {
	c.ticker.Stop()
	return nil
}
