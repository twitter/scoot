package cluster

import (
	log "github.com/sirupsen/logrus"

	"sort"
)

type state struct {
	// current view of our nodes
	nodes       map[NodeId]Node
	nopCheckCnt int
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
	oldStateLen := len(s.nodes)
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
		log.Infof("NodeAdded update: %s", n)
		outgoing = append(outgoing, NodeUpdate{
			UpdateType: NodeAdded,
			Id:         n.Id(),
			Node:       n,
		})
	}
	for _, n := range removed {
		log.Infof("NodeRemoved update: %s", n)
		outgoing = append(outgoing, NodeUpdate{
			UpdateType: NodeRemoved,
			Id:         n.Id(),
		})
	}

	// debugging scheduler performance issues: record when we see nodes being added removed
	// also record how many times we've checked and didn't see any changes (we're wondering if
	// this go routine is being swapped out for long periods of time).
	if len(added) > 0 || len(removed) > 0 {
		log.Infof("Number of nodes added: %d\nNumber of nodes removed: %d\n"+
			"Number of nodes in newState: %d\nNumber of nodes in old state: %d\n"+
			"(%d cluster checks with no change)", len(added), len(removed), len(newState), oldStateLen, s.nopCheckCnt)
		s.nopCheckCnt = 0
	} else {
		s.nopCheckCnt++
	}
	// reset nodes map, assign to new state
	s.nodes = make(map[NodeId]Node)
	for _, n := range newState {
		s.nodes[n.Id()] = n
	}
	return outgoing
}
