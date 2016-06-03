package distributor

import (
	msg "github.com/scootdev/scoot/messages"
	cm "github.com/scootdev/scoot/sched/clustermembership"
)

/*
 * Interface for choosing a node in a cluster to send
 * work to.  When DistributeWork is called a node from
 * the passed in cluster should be returned, where work
 * can be scheduled.
 *
 * work is the work to be scheduled
 * cluster is the Cluster to schedule the work on
 */
type Distributor interface {
	DistributeWork(work msg.Job, cluster cm.Cluster) cm.Node
}
