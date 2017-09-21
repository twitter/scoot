package scootconfig

import (
	"fmt"
	"time"

	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/cloud/cluster/local"
	"github.com/twitter/scoot/ice"
)

// Parameters for configuring an in-memory Scoot cluster
// Count - number of in-memory workers
type ClusterMemoryConfig struct {
	Type  string
	Count int
}

func (c *ClusterMemoryConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *ClusterMemoryConfig) Create() (*cluster.Cluster, error) {
	workerNodes := make([]cluster.Node, c.Count)
	for i := 0; i < c.Count; i++ {
		workerNodes[i] = cluster.NewIdNode(fmt.Sprintf("inmemory%d", i))
	}
	return cluster.NewCluster(workerNodes, nil), nil
}

// Parameters for configuring a Scoot cluster that will have locally-run components.
type ClusterLocalConfig struct {
	Type string
}

func (c *ClusterLocalConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *ClusterLocalConfig) Create() (*cluster.Cluster, error) {
	f := local.MakeFetcher("workerserver", "thrift_addr")
	updates := cluster.MakeFetchCron(f, time.NewTicker(time.Second).C)
	return cluster.NewCluster(nil, updates), nil
}
