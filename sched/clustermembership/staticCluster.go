package cluster_membership

/*
 * Represents a Cluster of nodes that is defined statically
 * Once created the nodes in the cluster never change
 *
 * Keeps a Slice of the NodeIds for fast returns on GetMembers
 * Keeps a Map of the nodeId to Node for fast lookup of nodes.
 */
type staticCluster struct {
	members []Node
}

func (c *staticCluster) Members() []Node {
	return c.members
}

/*
 * Creates a Static Cluster with the specified nodes.  Once
 * Defined the list of nodes in a static cluster can never be changed
 */
func StaticClusterFactory(nodes []Node) *staticCluster {
	return &staticCluster{
		members: nodes,
	}
}
