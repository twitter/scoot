package distributor

import (
	"github.com/scootdev/scoot/sched"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	"sync"
)

//
// Pool Distributor will assign work to any
// Unscheduled Node in the cluster.
//
// While a Node is doing work no new tasks will
// be sent to it.  Blocks until a node becomes free
//
// Static Cluster for now
//
type PoolDistributor struct {
	freeCh   chan cm.Node
	toRemove map[string]bool
	mutex    sync.RWMutex
}

//
// Creates a Pool Distributor, assumes static cluster &
// all nodes in the cluster are free upon creation
//
// If the cluster has 0 nodes in it returns nil
//
func NewPoolDistributor(cluster cm.Cluster) *PoolDistributor {

	nodes := cluster.Members()

	if len(nodes) <= 0 {
		return nil
	}

	// allocate the initial cluster size for the free channel
	// assuming static clusters next update will support
	// dynamic clusters
	freeCh := make(chan cm.Node, len(nodes))

	// put all the nodes in the freeCh
	for _, n := range nodes {
		freeCh <- n
	}

	return &PoolDistributor{
		freeCh: freeCh,
	}
}

//
// Creates a Pool Distributor, dynamic cluster &
// all nodes in the cluster are free upon creation
//
// If the cluster has 0 nodes in it returns nil
//
func NewDynamicPoolDistributor(cluster cm.DynamicCluster) *PoolDistributor {

	dist := NewPoolDistributor(cluster)
	dist.toRemove = make(map[string]bool)

	// spawn a go routine to deal with cluster updates
	go updateCluster(dist, cluster.NodeUpdates())

	return dist
}

//
// Watch for updates from the cluster and update internal list
//
func updateCluster(dist *PoolDistributor, updateCh <-chan cm.NodeUpdate) {
	for {
		select {

		case nodeUpdate := <-updateCh:
			switch nodeUpdate.UpdateType {
			case cm.NodeAdded:
				node := nodeUpdate.Node

				// Check if this node is on the remove watch list
				dist.mutex.RLock()
				_, ok := dist.toRemove[node.Id()]
				dist.mutex.RUnlock()

				// remove from remove watch list since we are re-adding it
				if ok {
					dist.mutex.Lock()
					// don't need to recheck condition because if the
					// element has already been removed from the watchlist
					// retrying to remove it is a safe operation, does nothing.
					delete(dist.toRemove, node.Id())
					dist.mutex.Unlock()
				}

				dist.freeCh <- node
			case cm.NodeRemoved:
				node := nodeUpdate.Node

				dist.mutex.Lock()
				dist.toRemove[node.Id()] = true
				dist.mutex.Unlock()
			}
		default:
		}
	}
}

//
// Retrieve a Node to Do Work for this Particular Job
//
func (dist *PoolDistributor) ReserveNode(work sched.Job) cm.Node {
	node := <-dist.freeCh

	// Check if the nodes we just got has been marked for removal
	nodeRemoved := true
	for nodeRemoved {

		// check if node is on the remove watchlist
		dist.mutex.RLock()
		_, nodeRemoved = dist.toRemove[node.Id()]
		dist.mutex.RUnlock()

		// we've successfully removed the node from the pool so delete from watchlist
		if nodeRemoved {
			dist.mutex.Lock()
			delete(dist.toRemove, node.Id())
			dist.mutex.Unlock()

			// wait for a new node and try again
			node = <-dist.freeCh
		}
	}

	return node
}

//
// Release a Node back into the pool to note that it has
// completed its assigned work
//
func (dist *PoolDistributor) ReleaseNode(node cm.Node) {

	// check if returned node has been added to delete watchlist
	dist.mutex.RLock()
	_, nodeRemoved := dist.toRemove[node.Id()]
	dist.mutex.RUnlock()

	// if node is on delete watchlist don't add back to freelist
	// remove node from watchlist since it's been removed
	if nodeRemoved {
		dist.mutex.Lock()
		delete(dist.toRemove, node.Id())
		dist.mutex.Unlock()

		// if node is not on remove watchlist just add it back to cluster
	} else {
		dist.freeCh <- node
	}
}
