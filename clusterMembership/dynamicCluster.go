package cluster_membership

/*
 * Represents a Cluster of nodes that can change over time.
 * Keeps a Map of the nodeId to Node for fast lookup of nodes.
 */
type dynamicCluster struct {
	members map[string]Node
}

/*
 * Returns a Snapshot of the Cluster Membership state
 */
func (c dynamicCluster) Members() []Node {

	nodes := make([]Node, len(c.members))
	index := 0

	for _, n := range c.members {
		nodes[index] = n
		index++
	}

	return nodes
}

/*
 * Adds A Node from the Cluster
 */
func (c dynamicCluster) AddNode(n Node) {
	c.members[n.Id()] = n
}

/*
 * Permanently Removes A Node from the Cluster
 */
func (c dynamicCluster) RemoveNode(nodeId string) {
	delete(c.members, nodeId)
}

/*
 * Creates a Dynamic Cluster with the an initial list of nodes
 * Dynamic Cluster can be moodified by adding or removing nodes.
 */
func DynamicClusterFactory(initialNodes []Node) dynamicCluster {
	membersMap := make(map[string]Node)

	for _, node := range initialNodes {
		membersMap[node.Id()] = node
	}

	return dynamicCluster{
		members: membersMap,
	}
}
