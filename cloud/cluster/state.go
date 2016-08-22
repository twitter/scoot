package cluster

import (
	"sort"
)

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
	sort.Sort(NodeUpdateSorter(outgoing))
	return outgoing
}

func (s *State) FilterAndUpdate(newUpdates []NodeUpdate) []NodeUpdate {
	filtered := []NodeUpdate{}
	for _, update := range newUpdates {
		_, ok := s.Nodes[update.Id]
		switch {
		case update.UpdateType == NodeAdded:
			if ok {
				// node is already included in state
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
				continue
			}
		}
	}
	return filtered
}
