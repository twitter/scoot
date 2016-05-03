package distributor

import cm "scootdev/scoot/clustermembership"

/*
 * Interface for choosing a node in a cluster to send
 * work to.  When DistributeWork is called a node from
 * the passed in cluster should be returned, where work
 * can be scheduled.
 */
type Distributor interface {
	DistributeWork(cluster cm.Cluster) cm.Node
}
