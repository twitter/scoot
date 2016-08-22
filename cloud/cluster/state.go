package cluster

import (
	"sort"
)

type State struct {
	// current view of our nodes
	Nodes 	map[NodeId]Node
}

func MakeState() *State {
	s := &State{
		Nodes: make(map[NodeId]Node),
	}
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
			Id: 		n.Id(),
			Node: 		n,
		})
	}
	for _, n := range removed {
		outgoing = append(outgoing, NodeUpdate{
			UpdateType: NodeRemoved,
			Id: 		n.Id(),
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

func (s *State) Update(newUpdates []NodeUpdate) {
	for _, update := range newUpdates {
		switch {
			case update.UpdateType == NodeAdded:
				s.Nodes[update.Id] = update.Node
			case update.UpdateType == NodeRemoved:
				delete(s.Nodes, update.Id)
		}
	}
}