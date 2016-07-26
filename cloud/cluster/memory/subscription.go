package memory

import (
	"github.com/scootdev/scoot/cloud/cluster"
)

// subscriber promptly reads its input and maintains a queue so that the sender
// doesn't have to worry about a send blocking.
type subscriber struct {
	inCh  chan []cluster.NodeUpdate
	outCh chan []cluster.NodeUpdate
	cl    *simpleCluster
	queue []cluster.NodeUpdate
}

func makeSubscription(initial []cluster.Node, cl *simpleCluster, inCh chan []cluster.NodeUpdate) cluster.Subscription {
	s := &subscriber{
		inCh:  inCh,
		outCh: make(chan []cluster.NodeUpdate),
		cl:    cl,
		queue: nil,
	}
	go s.loop()
	return cluster.Subscription{
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
		var outCh chan []cluster.NodeUpdate
		var outgoing []cluster.NodeUpdate
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
