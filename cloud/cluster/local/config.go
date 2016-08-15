package local

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/memory"
)

type ClusterLocalConfig struct {
	RegexCapturePort string
}

func (c *ClusterLocalConfig) Create() (cluster.Cluster, error) {
	sub := Subscribe(c.RegexCapturePort)
	return memory.NewCluster(sub.InitialMembers, sub.Updates), nil
}
