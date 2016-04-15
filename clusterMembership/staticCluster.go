package clusterMembership

import "fmt"
import "errors"

/*
 * Represents a Cluster of nodes that is defined statically
 * Once created the nodes in the cluster never change
 *
 * Keeps a Slice of the NodeIds for fast returns on GetMembers
 * Keeps a Map of the nodeId to Node for fast lookup of nodes.
 */
type staticCluster struct {
	members    []string
	membersMap map[string]Node
}

func (c staticCluster) GetMembers() []string {

	return c.members
}

func (c staticCluster) SendMessage(msg string, nodeId string) error {

	n, exists := c.membersMap[nodeId]

	if exists {
		n.SendMessage(msg)
		return nil
	} else {
		return errors.New(fmt.Sprintf("staticCluster: node %s is not in the cluster", nodeId))
	}
}

/*
 * Creates a Static Cluster with the specified nodes.  Once
 * Defined the list of nodes in a static cluster can never be changed
 */
func StaticClusterFactory(nodes []Node) staticCluster {

	// make a copy to return, so users see a snapshot of cluster state
	var members = make([]string, len(nodes))
	for i, n := range nodes {
		members[i] = n.GetId()
	}

	var membersMap = make(map[string]Node)

	for _, node := range nodes {
		membersMap[node.GetId()] = node
	}

	return staticCluster{
		members:    members,
		membersMap: membersMap,
	}
}
