package cluster_membership

/*
 * Represents a Cluster of nodes that can change over time.
 * Keeps a Map of the nodeId to Node for fast lookup of nodes.
 */
type dynamicCluster struct {
	membersList []Node
}

/*
 * Returns a Snapshot of the Cluster Membership state
 */
func (c *dynamicCluster) Members() []Node {
	return c.membersList
}

/*
 * Adds A Node from the Cluster
 */
func (c *dynamicCluster) AddNode(n Node) {
	c.membersList = append(c.membersList, n)
}

/*
 * Permanently Removes A Node from the Cluster
 */
func (c *dynamicCluster) RemoveNode(nodeId string) {
	var indexToDelete int = -1
	for i, node := range c.membersList {
		if node.Id() == nodeId {
			indexToDelete = i
			break
		}
	}

	if indexToDelete >= 0 {
		c.membersList, c.membersList[len(c.membersList)-1] = append(c.membersList[:indexToDelete], c.membersList[indexToDelete+1:]...), nil
	}
}

/*
 * Creates a Dynamic Cluster with the an initial list of nodes
 * Dynamic Cluster can be moodified by adding or removing nodes.
 */
func DynamicClusterFactory(initialNodes []Node) *dynamicCluster {
	var membersList []Node

	for _, node := range initialNodes {
		membersList = append(membersList, node)
	}

	return &dynamicCluster{
		membersList: membersList,
	}
}
