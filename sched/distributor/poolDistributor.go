package distributor

import (
	"github.com/scootdev/scoot/cloud/cluster"
)

// Pool Distributor offers leases on Nodes in the cluster. Each Node is either reserved or free.
type PoolDistributor struct {
	// Input/Output
	Reserve  chan cluster.Node
	Release  chan cluster.Node
	updateCh chan []cluster.NodeUpdate

	// Our state, read and written only by loop() and its subroutines
	nodes map[cluster.NodeId]cluster.Node
	free  []cluster.Node
}

// Creates a Pool Distributor, with all nodes free.
func NewPoolDistributor(initial []cluster.Node, updateCh chan []cluster.NodeUpdate) *PoolDistributor {
	nodes := make(map[cluster.NodeId]cluster.Node)
	free := []cluster.Node{}
	for _, n := range initial {
		nodes[n.Id()] = n
		free = append(free, n)
	}

	dist := &PoolDistributor{
		Reserve:  make(chan cluster.Node),
		Release:  make(chan cluster.Node),
		updateCh: updateCh,
		nodes:    nodes,
		free:     free,
	}
	go dist.loop()
	return dist
}

func NewPoolDistributorFromCluster(cluster cluster.Cluster) (*PoolDistributor, error) {
	sub := cluster.Subscribe()
	return NewPoolDistributor(sub.InitialMembers, sub.Updates), nil
}

func (d *PoolDistributor) Close() {
	close(d.Release)
}

// Serves pool distributor logic
func (d *PoolDistributor) loop() {
	for {
		var reserveCh chan cluster.Node
		var outgoing cluster.Node
		if len(d.free) > 0 {
			reserveCh, outgoing = d.Reserve, d.free[0]
		}
		select {
		case reserveCh <- outgoing:
			d.free = d.free[1:]
		case node, ok := <-d.Release:
			if !ok {
				return
			}
			if _, ok := d.nodes[node.Id()]; ok {
				d.free = append(d.free, node)
			}
		case updates, ok := <-d.updateCh:
			if !ok {
				d.updateCh = nil
				continue
			}
			d.update(updates)
		}
	}
}

// Update updates state in response to node updates from the cluster.
// (Must be called in the distributor's goroutine)
func (d *PoolDistributor) update(updates []cluster.NodeUpdate) {
	for _, update := range updates {
		switch update.UpdateType {
		case cluster.NodeAdded:
			d.nodes[update.Id] = update.Node
			d.free = append(d.free, update.Node)
		case cluster.NodeRemoved:
			delete(d.nodes, update.Id)
			for i, n := range d.free {
				if n.Id() == update.Id {
					d.free = append(d.free[:i], d.free[i+1:]...)
				}
			}
		}
	}
}
