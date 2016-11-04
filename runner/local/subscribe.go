package local

type Subscription struct {
	InCh    chan runner.ProcessStatus
	OutCh   chan runner.Event
	SinceCh chan runner.EventID

	subID runner.SubscriptionID

	first  int
	last   int
	events []runner.Event
}

func (s *Subscription) loop() {
	for {
		var outgoing Event
		var dest chan runner.Event

		if len(s.events) > 0 {
			outgoing, dest = events[0], s.OutCh
		}
		select {
		case id, ok := <-s.SinceCh:
			if !ok {
				s.SinceCh = nil
				continue
			}
			if id.Seq > s.first {
				s.first = id.Seq
			}
		case st, ok := <-s.InCh:
			if !ok {
				close(s.outCh)
				return
			}
			id := runner.EventID{Sub: s.subID, Seq: s.last}
			s.last++
			s.events = append(s.events, runner.Event{ID: id, Status: st})
		case dest <- outgoing:
			s.events = s.events[1:]
			s.first++
		}
		s.updateEvents()
	}
}
