package distributor

import (
	"github.com/scootdev/scoot/sched"
	cm "github.com/scootdev/scoot/sched/clustermembership"
)

/*
 * RoundRobinDistributor, evenly distributes load throughout the
 * cluster via a round robin process.
 */
type RoundRobin struct {
	currentIndex int
}

func (r *RoundRobin) DistributeWork(work sched.Job, cluster cm.Cluster) cm.Node {
	nodes := cluster.Members()

	r.currentIndex++
	if r.currentIndex >= len(nodes) {
		r.currentIndex = 0
	}

	return nodes[r.currentIndex]
}
