package distributor

import (
	msg "github.com/scootdev/scoot/messages"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	"math/rand"
)

/*
 * RandomDistributor, randomly selects a node from the cluster
 */
type Random struct{}

func (r *Random) DistributeWork(work msg.Job, cluster cm.Cluster) cm.Node {
	nodes := cluster.Members()
	index := rand.Intn(len(nodes))

	return nodes[index]
}
