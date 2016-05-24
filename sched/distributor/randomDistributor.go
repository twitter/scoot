package distributor

import "math/rand"
import msg "github.com/scootdev/scoot/messages"
import cm "github.com/scootdev/scoot/sched/clustermembership"

/*
 * RandomDistributor, randomly selects a node from the cluster
 */
type Random struct{}

func (r *Random) DistributeWork(work msg.Job, cluster cm.Cluster) cm.Node {
	nodes := cluster.Members()
	index := rand.Intn(len(nodes))

	return nodes[index]
}
