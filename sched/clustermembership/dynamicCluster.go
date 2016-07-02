package cluster_membership

//
// Represents a Cluster of nodes that can change over time.
// Keeps a Map of the nodeId to Node for fast lookup of nodes.
//
type dynamicCluster struct {
	memberSet   map[string]bool
	membersList []Node
	updateCh    chan NodeUpdate
	watcher     bool
}

//
// Returns a Snapshot of the Cluster Membership state
//
func (c *dynamicCluster) Members() []Node {
	return c.membersList
}

//
// Returns a Channel which contains Node Updates
//
func (c *dynamicCluster) NodeUpdates() <-chan NodeUpdate {
	c.watcher = true
	return c.updateCh
}

//
// Adds A Node to the Cluster
//
func (c *dynamicCluster) AddNode(n Node) {

	_, ok := c.memberSet[n.Id()]
	if !ok {
		c.membersList = append(c.membersList, n)
		c.memberSet[n.Id()] = true

		// don't add to channel and block unless someone is subscribed to updates
		if c.watcher {
			c.updateCh <- NodeUpdate{
				UpdateType: NodeAdded,
				Node:       n,
			}
		}
	}
}

//
// Permanently Removes A Node from the Cluster
//
func (c *dynamicCluster) RemoveNode(nodeId string) {
	var indexToDelete int = -1

	var nodeToDelete Node
	for i, node := range c.membersList {
		if node.Id() == nodeId {
			indexToDelete = i
			nodeToDelete = node
			break
		}
	}

	if indexToDelete >= 0 {
		c.membersList, c.membersList[len(c.membersList)-1] = append(c.membersList[:indexToDelete], c.membersList[indexToDelete+1:]...), nil
		delete(c.memberSet, nodeId)

		// don't add to channel and block unless someone is subscribed to updates
		if c.watcher {
			c.updateCh <- NodeUpdate{
				UpdateType: NodeRemoved,
				Node:       nodeToDelete,
			}
		}
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
		updateCh:    make(chan NodeUpdate, 1),
		watcher:     false,
	}
}
