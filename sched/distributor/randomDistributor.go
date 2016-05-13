package distributor

import "math/rand"
import cm "github.com/scootdev/scoot/sched/clustermembership"

/*
 * RandomDistributor, randomly selects a node from the cluster
 */
type Random struct{}

func (r *Random) DistributeWork(work string, cluster cm.Cluster) cm.Node {
	nodes := cluster.Members()
	index := rand.Intn(len(nodes))

	return nodes[index]
}
