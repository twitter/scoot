package cluster

// Subscriber receives node updates from its cluster and maintains a queue
// so that the sender doesn't have to worry about send blocking

type Subscriber struct {
	inCh  chan []NodeUpdate
	OutCh chan []NodeUpdate
	cl    *Cluster
	queue []NodeUpdate
}

func newSubscriber(initial []Node, cl *Cluster, inCh chan []NodeUpdate) Subscriber {
	s := Subscriber{
		inCh:  inCh,
		OutCh: make(chan []NodeUpdate),
		cl:    cl,
		queue: nil,
	}
	go s.loop()
	return s
}

func (s *Subscriber) Close() error {
	s.cl.closeSubscription(s)
	return nil
}

func (s *Subscriber) loop() {
	for s.inCh != nil || len(s.queue) > 0 {
		var outCh chan []NodeUpdate
		if len(s.queue) > 0 {
			outCh = s.OutCh
		}
		select {
		case updates, ok := <-s.inCh:
			if !ok {
				s.inCh = nil
				continue
			}
			s.queue = append(s.queue, updates...)
		case outCh <- s.queue:
			s.queue = nil
		}
	}
	close(s.OutCh)
}
