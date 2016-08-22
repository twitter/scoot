package cluster

type Subscriber struct {
	inCh  	chan []NodeUpdateb
	OutCh 	chan []NodeUpdate
	cl    	*simpleCluster
	queue 	[]NodeUpdate
}

func newSubscriber(initial []Node, cl *simpleCluster, inCh chan []NodeUpdate) Subscriber {
	s := Subscriber{
		inCh:  	inCh,
		OutCh: 	make(chan []NodeUpdate),
		cl:    	cl,
		queue: 	nil,
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
		var outgoing []NodeUpdate
		if len(s.queue) > 0 {
			outCh = s.OutCh
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
	close(s.OutCh)
}
