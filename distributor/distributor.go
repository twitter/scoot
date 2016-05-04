package distributor

import cm "scootdev/scoot/clustermembership"

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
	DistributeWork(work string, cluster cm.Cluster) cm.Node
}
