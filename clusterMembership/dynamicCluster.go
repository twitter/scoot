package cluster_membership

import "fmt"
import "errors"

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
func (c dynamicCluster) GetMembers() []string {

	var nodes = make([]string, len(c.members))
	var index = 0

	for _, n := range c.members {
		nodes[index] = n.GetId()
		index++
	}

	return nodes
}

/*
 * Adds A Node from the Cluster
 */
func (c dynamicCluster) AddNode(n Node) {
	c.members[n.GetId()] = n
}

/*
 * Permanently Removes A Node from the Cluster
 */
func (c dynamicCluster) RemoveNode(nodeId string) {
	delete(c.members, nodeId)
}

/*
 * Sends a Message to the specified node
 */
func (c dynamicCluster) SendMessage(msg string, nodeId string) error {
	n, exists := c.members[nodeId]

	if exists {
		n.SendMessage(msg)
		return nil
	} else {
		return errors.New(fmt.Sprintf("dynamicCluster: node %s is not in the cluster", nodeId))
	}
}

/*
 * Creates a Dynamic Cluster with the an initial list of nodes
 * Dynamic Cluster can be moodified by adding or removing nodes.
 */
func DynamicClusterFactory(initialNodes []Node) dynamicCluster {
	var membersMap = make(map[string]Node)

	for _, node := range initialNodes {
		membersMap[node.GetId()] = node
	}

	return dynamicCluster{
		members: membersMap,
	}
}
