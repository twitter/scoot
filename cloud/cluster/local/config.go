package local

import (
	"github.com/scootdev/scoot/cloud/cluster"
)

type ClusterLocalConfig struct {
	Type string
}

func (c *ClusterLocalConfig) Create() (*cluster.Cluster, error) {
	sub, fetcher := Subscribe()
	return cluster.NewCluster(sub.InitialMembers, sub.Updates, make(chan interface{}), fetcher), nil
}
