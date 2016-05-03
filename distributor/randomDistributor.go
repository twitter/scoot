package distributor

import "math/rand"
import cm "scootdev/scoot/clustermembership"

/*
 * RandomDistributor, randomly selects a node from the cluster
 */
type Random struct{}

func (r *Random) DistributeWork(cluster cm.Cluster) cm.Node {
	nodes := cluster.Members()
	index := rand.Intn(len(nodes))

	return nodes[index]
}
