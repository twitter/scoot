package cluster

type State struct {
	// current view of our nodes
	Nodes map[NodeId]Node
}

func MakeState(nodes []Node) *State {
	s := &State{
		Nodes: make(map[NodeId]Node),
	}
	s.SetAndDiff(nodes)
	return s
}

// SetAndDiff takes the new state as an argument and creates
// node updates based on the diff
func (s *State) SetAndDiff(newState []Node) []NodeUpdate {
	added := []Node{}
	for _, n := range newState {
		if _, exists := s.Nodes[n.Id()]; exists {
			// remove from s.nodes so that s.nodes only contains nodes removed in this diff
			delete(s.Nodes, n.Id())
		} else {
			added = append(added, n)
		}
	}
	removed := []Node{}
	for _, n := range s.Nodes {
		removed = append(removed, n)
	}

	outgoing := []NodeUpdate{}
	for _, n := range added {
		outgoing = append(outgoing, NodeUpdate{
			UpdateType: NodeAdded,
			Id:         n.Id(),
			Node:       n,
		})
	}
	for _, n := range removed {
		outgoing = append(outgoing, NodeUpdate{
			UpdateType: NodeRemoved,
			Id:         n.Id(),
		})
	}
	// reset nodes map, assign to new state
	s.Nodes = make(map[NodeId]Node)
	for _, n := range newState {
		s.Nodes[n.Id()] = n
	}
	// present updates in predictable order
	// sort.Sort(NodeUpdateSorter(outgoing))
	return outgoing
}

// FilterAndUpdate takes node updates as an argument and applies them
// to the state to create a new state. It also filters out updates that
// are not applicable, i.e. adding a node that is already in the state
// or removing one that isn't present.
func (s *State) FilterAndUpdate(newUpdates []NodeUpdate) []NodeUpdate {
	unused := []NodeUpdate{}
	filtered := []NodeUpdate{}
	for _, update := range newUpdates {
		_, ok := s.Nodes[update.Id]
		switch {
		case update.UpdateType == NodeAdded:
			if ok {
				// node is already included in state
				unused = append(unused, update)
				continue
			} else {
				// add node to state
				s.Nodes[update.Id] = update.Node
				filtered = append(filtered, update)
			}
		case update.UpdateType == NodeRemoved:
			if ok {
				// remove node from state
				delete(s.Nodes, update.Id)
				filtered = append(filtered, update)
			} else {
				// node wasn't previously in state
				unused = append(unused, update)
				continue
			}
		}
	}
	if len(unused) == 0 || len(unused) == len(newUpdates) {
		// if all updates were applied or if none were applied
		// return the filtered updates
		return filtered
	} else {
		// recurse through until either all unused updates are applied
		// or none are
		next := s.FilterAndUpdate(unused)
		for _, nextUpdate := range next {
			filtered = append(filtered, nextUpdate)
		}
		return filtered
	}
}
