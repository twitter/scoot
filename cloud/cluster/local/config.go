package local

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/memory"
)

type ClusterLocalConfig struct {
	Type string
}

func (c *ClusterLocalConfig) Create() (cluster.Cluster, error) {
	sub := Subscribe()
	return memory.NewCluster(sub.InitialMembers, sub.Updates), nil
}
