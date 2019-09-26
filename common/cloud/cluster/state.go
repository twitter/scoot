package cluster

import (
	"sort"
)

type state struct {
	// current view of our nodes
	nodes map[NodeId]Node
}

func makeState(nodes []Node) *state {
	s := &state{
		nodes: make(map[NodeId]Node),
	}
	s.setAndDiff(nodes)
	return s
}

// SetAndDiff takes the new state as an argument and creates
// node updates based on the diff
func (s *state) setAndDiff(newState []Node) []NodeUpdate {
	added := []Node{}
	for _, n := range newState {
		if _, exists := s.nodes[n.Id()]; exists {
			// remove from s.nodes so that s.nodes only contains nodes removed in this diff
			delete(s.nodes, n.Id())
		} else {
			added = append(added, n)
		}
	}
	removed := []Node{}
	for _, n := range s.nodes {
		removed = append(removed, n)
	}
	sort.Sort(NodeSorter(added))
	sort.Sort(NodeSorter(removed))
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
	s.nodes = make(map[NodeId]Node)
	for _, n := range newState {
		s.nodes[n.Id()] = n
	}
	return outgoing
}

// FilterAndUpdate takes node updates as an argument and applies them
// to the state to create a new state. It also filters out updates that
// are not applicable, i.e. adding a node that is already in the state
// or removing one that isn't present.
func (s *state) filterAndUpdate(newUpdates []NodeUpdate) []NodeUpdate {
	unused := []NodeUpdate{}
	filtered := []NodeUpdate{}
	for _, update := range newUpdates {
		_, ok := s.nodes[update.Id]
		switch {
		case update.UpdateType == NodeAdded:
			if ok {
				// node is already included in state
				unused = append(unused, update)
				continue
			} else {
				// add node to state
				s.nodes[update.Id] = update.Node
				filtered = append(filtered, update)
			}
		case update.UpdateType == NodeRemoved:
			if ok {
				// remove node from state
				delete(s.nodes, update.Id)
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
		next := s.filterAndUpdate(unused)
		for _, nextUpdate := range next {
			filtered = append(filtered, nextUpdate)
		}
		return filtered
	}
}
