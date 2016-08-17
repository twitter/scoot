package memory

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"sort"
)

type Differ struct {
	// current view of our nodes
	nodes 	map[string]cluster.Node
	// NodeUpdates that are ready and waiting to send
	outgoing cluster.NodeUpdates
}

func MakeDiffer() *Differ {
	d := &Differ{
		nodes: 	 	make(map[string]cluster.Node),
		outgoing: 	cluster.NodeUpdates{},
	}
	return d
}

func (d *Differ) MakeDiff(current []cluster.Node) cluster.NodeUpdates {
	d.outgoing = nil
	d.setAndDiff(current)
	return d.outgoing
}

func (d *Differ) setAndDiff(nodes []cluster.Node) {
	next := make(map[string]cluster.Node)
	added := []cluster.Node{}
	for _, n := range nodes {
		id := string(n.Id())
		next[id] = n
		if _, exists := d.nodes[id]; exists {
			// remove from d.nodes so that d.nodes only contains nodes removed in this diff
			delete(d.nodes, id)
		} else {
			added = append(added, n)
		}
	}
	removed := []cluster.Node{}
	for _, n := range d.nodes {
		removed = append(removed, n)
	}

	for _, n := range added {
		d.outgoing = append(d.outgoing, cluster.NodeUpdate{
			UpdateType: cluster.NodeAdded,
			Id: 		n.Id(),
			Node: 		n,
		})
	}
	for _, n := range removed {
		d.outgoing = append(d.outgoing, cluster.NodeUpdate{
			UpdateType: cluster.NodeRemoved,
			Id: 		n.Id(),
		})
	}
	// present updates in predictable order
	sort.Sort(d.outgoing)
	// keys of current nodes map are now the nodes passed in arg
	d.nodes = next
}