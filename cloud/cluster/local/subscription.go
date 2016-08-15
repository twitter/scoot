// Local offers data from the unix system processes list as Scoot Cluster Membership
package local

import "github.com/scootdev/scoot/cloud/cluster"

// Subscribe subscribes to node updates on the specified interval.
func Subscribe() cluster.Subscription {
	// TODO(dbentley): we could also take a context and stop the subscription when
	// the context is done for proper shutdown.
	fetcher := MakeFetcher()
	return cluster.Subscribe(fetcher)
}
