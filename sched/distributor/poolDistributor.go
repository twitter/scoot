package distributor

import (
	"github.com/scootdev/scoot/sched"
	cm "github.com/scootdev/scoot/sched/clustermembership"
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
	freeCh chan cm.Node
}

//
// Creates a Pool Distributor, assumes
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
// Retrieve a Node to Do Work for this Particular Job
//
func (dist *PoolDistributor) ReserveNode(work sched.Job) cm.Node {
	return <-dist.freeCh
}

//
// Release a Node back into the pool to note that it has
// completed its assigned work
//
func (dist *PoolDistributor) ReleaseNode(node cm.Node) {
	dist.freeCh <- node
}
