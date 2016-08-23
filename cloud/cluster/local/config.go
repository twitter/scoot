package local

import (
	"github.com/scootdev/scoot/cloud/cluster"
)

type ClusterLocalConfig struct {
	Type string
}

func (c *ClusterLocalConfig) Create() (*cluster.Cluster, error) {
	return cluster.NewCluster([]cluster.Node{}, make(chan interface{})), nil
}
