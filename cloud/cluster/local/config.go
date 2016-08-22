package local

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/memory"
)

type ClusterLocalConfig struct {
	Type string
}

func (c *ClusterLocalConfig) Create() (cluster.Cluster, error) {
	sub := cluster.Subscribe(MakeFetcher())
	stateCh := make(chan []cluster.Node)
	return memory.NewCluster(sub.InitialMembers, sub.Updates, stateCh), nil
}
