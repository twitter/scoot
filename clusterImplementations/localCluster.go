package cluster_implementations

import "fmt"
import cm "scootdev/scoot/clustermembership"

/*
 * localNode ClusterMember are for test purposes.
 * Simulates a node locally and just prints all
 * received messages
 */
type localNode struct {
	name string
}

func (n localNode) SendMessage(msg string) error {
	fmt.Println(n.name, "received message", msg)
	return nil
}

func (n localNode) GetId() string {
	return n.name
}

/*
 * Creates a Static LocalNode Cluster with the specified
 * Number of Nodes in it.
 */
func StaticLocalNodeClusterFactory(size int) cm.Cluster {
	var nodes = make([]cm.Node, size)

	for s := 0; s < size; s++ {
		nodes[s] = localNode{
			name: fmt.Sprintf("static_node_%d", s),
		}
	}

	return cm.StaticClusterFactory(nodes)
}

/*
 * Creates a Dynamic LocalNode Cluster with the initial
 * number of Nodes in it.
 */
func DynamicLocalNodeClusterFactory(initialSize int) cm.Cluster {
	var nodes = make([]cm.Node, initialSize)

	for s := 0; s < initialSize; s++ {
		nodes[s] = localNode{
			name: fmt.Sprintf("dynamic_node_%d", s),
		}
	}

	return cm.DynamicClusterFactory(nodes)
}
