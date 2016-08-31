package local

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"time"
)

type ClusterLocalConfig struct {
	Type string
}

func (c *ClusterLocalConfig) Create() (*cluster.Cluster, error) {
	f := MakeFetcher()
	updates := cluster.MakeFetchCron(f, time.NewTicker(time.Second).C)
	return cluster.NewCluster(nil, updates), nil
}
