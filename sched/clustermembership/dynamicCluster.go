package cluster_membership

/*
 * Represents a Cluster of nodes that can change over time.
 * Keeps a Map of the nodeId to Node for fast lookup of nodes.
 */
type dynamicCluster struct {
	memberSet   map[string]bool
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

	_, ok := c.memberSet[n.Id()]
	if !ok {
		c.membersList = append(c.membersList, n)
		c.memberSet[n.Id()] = true
	}

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
		delete(c.memberSet, nodeId)
	}
}

/*
 * Creates a Dynamic Cluster with the an initial list of nodes
 * Dynamic Cluster can be moodified by adding or removing nodes.
 */
func DynamicClusterFactory(initialNodes []Node) *dynamicCluster {
	var membersList []Node
	memberSet := make(map[string]bool)

	for _, node := range initialNodes {
		membersList = append(membersList, node)
		memberSet[node.Id()] = true
	}

	return &dynamicCluster{
		membersList: membersList,
		memberSet:   memberSet,
	}
}
